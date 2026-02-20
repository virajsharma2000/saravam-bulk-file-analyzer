"""
sarvam_client.py — Async client for Sarvam Document Intelligence API.
Handles the full async workflow: Create Job -> Upload -> Start -> Poll -> Download -> Extract.
"""
from __future__ import annotations

import asyncio
import logging
import json
import os
import io
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
import aiofiles

from config import Config
from models import ExtractionResult
from utils import exponential_backoff

logger = logging.getLogger(__name__)


class SarvamClientError(Exception):
    """Custom exception for Sarvam API errors."""
    pass


def _get_headers() -> Dict[str, str]:
    return {
        "api-subscription-key": Config.SARVAM_API_KEY,
    }


async def _create_job(client: httpx.AsyncClient) -> str:
    """
    Step 0: Create a new document intelligence job.
    """
    url = f"{Config.SARVAM_DOC_ENDPOINT}" # Base URL .../job/v1
    
    payload = {
        "storage_container_type": "Azure",
        "job_parameters": {
            "language_code": "en-IN", 
            "output_format": "md"
        }
    }

    logger.info(f"Creating Sarvam job at {url}...")
    
    try:
        response = await client.post(url, headers=_get_headers(), json=payload)
        response.raise_for_status()
        data = response.json()
        job_id = data.get("job_id")
        if not job_id:
             raise SarvamClientError(f"Job creation response missing job_id: {data}")
        logger.info(f"Job created: {job_id}")
        return job_id
    except httpx.HTTPStatusError as e:
        logger.error(f"Failed to create job: {e.response.text}")
        raise SarvamClientError(f"Create job failed: {e}")
    except httpx.RequestError as e:
        logger.error(f"Network error creating job: {e}")
        raise SarvamClientError(f"Network error: {e}")


async def _get_upload_urls(client: httpx.AsyncClient, job_id: str, file_path: str) -> Dict[str, Any]:
    """
    Step 1: Get presigned upload URLs.
    """
    url = f"{Config.SARVAM_DOC_ENDPOINT}/upload-files"
    filename = os.path.basename(file_path)
    
    # Now we include job_id
    payload = {
        "job_id": job_id,
        "files": [filename]
    }
    
    logger.info(f"Requesting upload URL for {filename} (Job {job_id})...")
    
    try:
        response = await client.post(url, headers=_get_headers(), json=payload)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Failed to get upload URLs: {e.response.text}")
        raise SarvamClientError(f"Upload URL request failed: {e}")


async def _upload_to_blob(client: httpx.AsyncClient, upload_url: str, file_path: str):
    """
    Step 2: Upload file to the provided blob URL.
    """
    logger.info(f"Uploading {file_path} to blob storage...")
    
    ext = os.path.splitext(file_path)[1].lower()
    content_type = "application/pdf" if ext == ".pdf" else "application/octet-stream"
    
    try:
        async with aiofiles.open(file_path, "rb") as f:
            content = await f.read()
            
        response = await client.put(
            upload_url, 
            content=content, 
            headers={
                "Content-Type": content_type,
                "x-ms-blob-type": "BlockBlob"
            }
        )
        response.raise_for_status()
        logger.info(f"Upload of {file_path} successful.")
        
    except (httpx.HTTPStatusError, httpx.RequestError, IOError) as e:
        logger.error(f"Failed to upload file to blob: {e}")
        raise SarvamClientError(f"Blob upload failed: {e}")


async def _start_job(client: httpx.AsyncClient, job_id: str) -> Dict[str, Any]:
    """
    Step 3: Start the processing job.
    """
    url = f"{Config.SARVAM_DOC_ENDPOINT}/{job_id}/start"
    logger.info(f"Starting job {job_id}...")
    
    try:
        response = await client.post(url, headers=_get_headers())
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Failed to start job {job_id}: {e.response.text}")
        raise SarvamClientError(f"Start job failed: {e}")


async def _poll_job(client: httpx.AsyncClient, job_id: str) -> Dict[str, Any]:
    """
    Step 4: Poll job status until completion or failure.
    """
    url = f"{Config.SARVAM_DOC_ENDPOINT}/{job_id}/status"
    logger.info(f"Polling job {job_id}...")
    
    import time
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > Config.SARVAM_POLLING_TIMEOUT:
             raise SarvamClientError(f"Job {job_id} timed out after {Config.SARVAM_POLLING_TIMEOUT}s.")

        try:
            response = await client.get(url, headers=_get_headers())
            response.raise_for_status()
            data = response.json()
            state = data.get("job_state", "Unknown")
            
            if state == "Completed":
                logger.info(f"Job {job_id} completed successfully.")
                return data
            
            if state == "Failed":
                error_msg = data.get("error_message", "Unknown error")
                logger.error(f"Job {job_id} failed: {error_msg}")
                raise SarvamClientError(f"Job failed: {error_msg}")
            
            # Dynamic sleep? Or fixed? Fixed 2s is fine but maybe slightly longer for long jobs?
            await asyncio.sleep(2.0)
            
        except httpx.HTTPStatusError as e:
            logger.warning(f"Poll request failed: {e}")
            await asyncio.sleep(2.0)


async def _get_download_url_and_extract(client: httpx.AsyncClient, job_id: str) -> str:
    """
    Step 5: Get download URL, fetch ZIP, extract content.
    """
    # 5a. Get Download URL
    url = f"{Config.SARVAM_DOC_ENDPOINT}/{job_id}/download-files"
    logger.info(f"Requesting download URL for job {job_id}...")
    
    try:
        # POST method as verified
        response = await client.post(url, headers=_get_headers(), json={})
        response.raise_for_status()
        data = response.json()
        
        download_urls = data.get("download_urls", {})
        # Assuming structure: {"filename.zip": {"file_url": "..."}}
        if not download_urls:
            # Maybe directly in data?
            # Probe showed: "download_urls": { "document.zip": { "file_url": "..." } }
             logger.warning(f"No download URLs found in response: {data}")
             return ""

        # Extract the first file URL
        first_key = list(download_urls.keys())[0]
        file_info = download_urls[first_key]
        
        download_link = None
        if isinstance(file_info, dict):
            download_link = file_info.get("file_url")
        elif isinstance(file_info, str):
            download_link = file_info
            
        if not download_link:
            logger.error(f"Could not extract file_url for {first_key}: {file_info}")
            return ""
            
        logger.info(f"Downloading content from {first_key}...")
        
        # 5b. Download ZIP
        zip_resp = await client.get(download_link)
        zip_resp.raise_for_status()
        
        # 5c. Extract content
        with zipfile.ZipFile(io.BytesIO(zip_resp.content)) as z:
            # Look for markdown files
            md_files = [f for f in z.namelist() if f.endswith('.md')]
            json_files = [f for f in z.namelist() if f.endswith('.json')]
            
            target_file = None
            if md_files:
                target_file = md_files[0]
            elif json_files:
                target_file = json_files[0]
            else:
                 # Fallback: take any file
                 target_file = z.namelist()[0] if z.namelist() else None
                 
            if not target_file:
                logger.warning("Empty ZIP file or no recognizable content.")
                return ""
            
            logger.info(f"Extracting text from {target_file}...")
            text_content = z.read(target_file).decode('utf-8')
            return text_content
            
    except httpx.HTTPStatusError as e:
        logger.error(f"Download failed: {e.response.text if e.response else e}")
        raise SarvamClientError(f"Download failed: {e}")
    except zipfile.BadZipFile:
        logger.error("Failed to unzip response content.")
        raise SarvamClientError("Invalid ZIP file received.")
    except Exception as e:
        logger.error(f"Extraction error: {e}")
        raise SarvamClientError(f"Content extraction error: {e}")


async def extract_text(file_path: str) -> ExtractionResult:
    """
    Orchestrates the Sarvam Async Document Intelligence workflow.
    """
    try:
        async with httpx.AsyncClient(timeout=Config.HTTP_TIMEOUT) as client:
            # 0. Create Job
            job_id = await _create_job(client)
            
            # 1. Get Upload URLs
            upload_data = await _get_upload_urls(client, job_id, file_path)
            upload_urls = upload_data.get("upload_urls", {})
            
            filename = os.path.basename(file_path)
            
            upload_info = upload_urls.get(filename)
            upload_url = None
            
            if isinstance(upload_info, dict):
                upload_url = upload_info.get("file_url")
            elif isinstance(upload_info, str):
                upload_url = upload_info
            
            # Fallback
            if not upload_url and upload_urls:
                 first_val = list(upload_urls.values())[0]
                 if isinstance(first_val, dict):
                     upload_url = first_val.get("file_url")
                 elif isinstance(first_val, str):
                     upload_url = first_val
            
            if not upload_url:
                raise SarvamClientError(f"No upload URL found for {filename}")

            # 2. Upload to Blob
            await _upload_to_blob(client, upload_url, file_path)
            
            # 3. Start Job
            await _start_job(client, job_id)
            
            # 4. Poll for Result
            await _poll_job(client, job_id)
            
            # 5. Download and Extract Content
            logger.info(f"Job validated. Fetching content for job {job_id}...")
            extracted_text = await _get_download_url_and_extract(client, job_id)
            
            if not extracted_text:
                logger.warning(f"No text extracted for job {job_id}!")
                # Fallback to empty string, but log it clearly
            
            return ExtractionResult(
                text=extracted_text, 
                stats={"job_id": job_id, "status": "Completed"}
            )

    except SarvamClientError as e:
        logger.error(f"Sarvam extraction failed for {file_path}: {e}")
        return ExtractionResult(text="", stats={"error": str(e)})
    except Exception as e:
        logger.error(f"Unexpected error for {file_path}: {e}", exc_info=True)
        return ExtractionResult(text="", stats={"error": str(e)})
