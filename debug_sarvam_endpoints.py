import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SARVAM_API_KEY")
BASE_URL = "https://api.sarvam.ai/doc-digitization/job/v1"

if not API_KEY:
    print("Error: SARVAM_API_KEY not found in .env")
    exit(1)

headers = {
    "api-subscription-key": API_KEY,
    "Content-Type": "application/json"
}

# Use the completed job ID
JOB_ID = "20260220_4a6447a7-df74-44f9-ad85-05cc11a13257" 

def probe_download(job_id):
    # Potential endpoints
    urls_to_test_post = [
         # This returned 405 on GET, so likely POST is correct
        (f"{BASE_URL}/{job_id}/download-files", {}),
        (f"{BASE_URL}/{job_id}/download-links", {}),
    ]
    
    print(f"\nProbing POST Download endpoints for Job {job_id}...")

    for url, payload in urls_to_test_post:
        print(f"Testing POST {url} ...", end=" ")
        try:
            resp = requests.post(url, headers=headers, json=payload)
            print(f"Status: {resp.status_code}")
            if resp.status_code == 200:
                print(">>> FOUND CORRECT ENDPOINT! <<<")
                print(json.dumps(resp.json(), indent=2))
                return
            else:
                print(resp.text)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    probe_download(JOB_ID)
