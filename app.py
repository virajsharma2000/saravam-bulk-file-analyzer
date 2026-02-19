"""
app.py — Streamlit UI for the Bulk File Retention Analyzer.
Run with:  streamlit run app.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import streamlit as st
import pandas as pd

# ── Ensure repo root is on sys.path so sibling modules resolve ─────────────
sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from database import init_db, get_all_results
from file_scanner import scan_folder
from models import FileRecord
from action_engine import ActionEngine
import retention_engine

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("app")

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Bulk File Retention Analyzer",
    page_icon="🗂️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    [data-testid="stSidebar"] { background: #1a1a2e; }
    [data-testid="stSidebar"] * { color: #e0e0e0 !important; }
    .metric-card {
        background: #16213e;
        border-radius: 10px;
        padding: 16px;
        text-align: center;
        color: white;
    }
    .metric-card h2 { margin: 4px 0; font-size: 2rem; }
    .metric-card p  { margin: 0; font-size: 0.85rem; color: #aaa; }
    .stProgress > div > div { background: linear-gradient(90deg, #667eea, #764ba2); }
    .badge-delete  { background:#c0392b; color:white; padding:2px 8px; border-radius:8px; font-size:0.8rem; }
    .badge-archive { background:#e67e22; color:white; padding:2px 8px; border-radius:8px; font-size:0.8rem; }
    .badge-retain  { background:#27ae60; color:white; padding:2px 8px; border-radius:8px; font-size:0.8rem; }
    .badge-review  { background:#2980b9; color:white; padding:2px 8px; border-radius:8px; font-size:0.8rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── Session state initialisation ──────────────────────────────────────────────

def _init_state() -> None:
    defaults = {
        "db_conn": None,
        "scan_results": [],      # List[ScannedFile]
        "processed_records": [], # List[FileRecord]
        "logs": [],
        "running": False,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


_init_state()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_db() -> Any:
    if st.session_state.db_conn is None:
        st.session_state.db_conn = init_db()
    return st.session_state.db_conn


def _push_log(message: str) -> None:
    st.session_state.logs.append(message)


def _action_badge(action: str) -> str:
    css = {"delete": "badge-delete", "archive": "badge-archive",
           "retain": "badge-retain", "review": "badge-review"}.get(action, "badge-review")
    return f'<span class="{css}">{action.upper()}</span>'


def _results_to_df(results: List[Dict]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    # Human-readable file size
    if "file_size" in df.columns:
        df["file_size"] = df["file_size"].apply(
            lambda b: f"{b/1024:.1f} KB" if b < 1_048_576 else f"{b/1048576:.1f} MB"
        )
    # Shorten long paths for display
    if "file_path" in df.columns:
        df["display_path"] = df["file_path"].apply(lambda p: "…/" + "/".join(p.split("/")[-2:]))
    col_order = [
        "display_path", "suggested_action", "retention_score",
        "confidence", "category", "file_size", "last_modified",
        "reasoning", "processed_at",
    ]
    return df[[c for c in col_order if c in df.columns]]


def _run_async(coro) -> Any:
    """Run an async coroutine from sync Streamlit code."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Nest asyncio (needed in some environments)
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(coro)
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image(
        "https://img.icons8.com/fluency/96/000000/folder-invoices.png",
        width=64,
    )
    st.title("🗂️ Retention Analyzer")
    st.caption("Bulk AI-powered file retention decisions")
    st.divider()

    # ── Configuration ──
    st.subheader("⚙️ Configuration")
    api_key_input = st.text_input(
        "Sarvam API Key",
        type="password",
        placeholder="sk-...",
        help="Overrides SARVAM_API_KEY env variable",
    )
    if api_key_input:
        Config.SARVAM_API_KEY = api_key_input
        os.environ["SARVAM_API_KEY"] = api_key_input

    st.divider()

    # ── Scan target ──
    st.subheader("📁 Scan Target")
    folder_path = st.text_input(
        "Folder Path",
        placeholder="/home/user/documents",
        help="Supports jpg, jpeg, png, pdf",
    )

    st.divider()

    # ── Actions ──
    st.subheader("🚀 Actions")
    dry_run = st.toggle("Dry Run Mode", value=True, help="Preview actions without moving files")

    scan_btn = st.button(
        "🔍 Scan & Analyze",
        use_container_width=True,
        disabled=st.session_state.running,
        type="primary",
    )

    st.divider()

    # ── Apply actions ──
    st.subheader("⚡ Apply Actions")
    action_filter = st.multiselect(
        "Filter actions to apply",
        options=["delete", "archive", "retain", "review"],
        default=["delete", "archive"],
    )
    apply_btn = st.button(
        "✅ Apply Selected Actions",
        use_container_width=True,
        disabled=st.session_state.running,
    )


# ── Main panel ────────────────────────────────────────────────────────────────

st.title("🗂️ Bulk File Retention Analyzer")
st.caption("AI-powered retention analysis using Sarvam Document Intelligence + LLM classification")

# ── Run scan & analysis ───────────────────────────────────────────────────────
if scan_btn:
    if not folder_path:
        st.error("Please enter a folder path in the sidebar.")
    elif not Path(folder_path).is_dir():
        st.error(f"Directory not found: `{folder_path}`")
    elif not Config.SARVAM_API_KEY:
        st.error("Sarvam API Key is required. Enter it in the sidebar or set `SARVAM_API_KEY` env var.")
    else:
        st.session_state.running = True
        st.session_state.logs = []
        conn = _ensure_db()

        # ── Scanner phase ──
        with st.spinner("🔍 Scanning folder…"):
            try:
                scanned = scan_folder(folder_path)
                st.session_state.scan_results = scanned
                _push_log(f"✅ Found {len(scanned)} supported file(s) in `{folder_path}`")
            except (FileNotFoundError, NotADirectoryError) as exc:
                st.error(str(exc))
                st.session_state.running = False
                st.stop()

        if not scanned:
            st.warning("No supported files (jpg, jpeg, png, pdf) found in the selected folder.")
            st.session_state.running = False
            st.stop()

        # ── Analysis phase ──
        progress_bar = st.progress(0, text="Initialising…")
        log_placeholder = st.empty()
        processed_count = [0]
        total = len(scanned)

        def progress_callback(file_path: str, status: str) -> None:
            short = Path(file_path).name
            _push_log(f"  [{status.upper()}] {short}")
            if status == "done":
                processed_count[0] += 1
                pct = int(processed_count[0] / total * 100)
                progress_bar.progress(
                    min(pct, 100),
                    text=f"Processing {processed_count[0]}/{total} — {short}",
                )
            log_placeholder.code("\n".join(st.session_state.logs[-15:]), language="")

        try:
            records = _run_async(
                retention_engine.process_all(
                    scanned_files=scanned,
                    conn=conn,
                    progress_callback=progress_callback,
                )
            )
            st.session_state.processed_records = records
            progress_bar.progress(100, text="✅ Analysis complete!")
            _push_log(f"\n🎉 Done! Processed {len(records)} file(s).")
            log_placeholder.code("\n".join(st.session_state.logs[-20:]), language="")
        except Exception as exc:
            st.error(f"Analysis failed: {exc}")
            logger.exception("Analysis pipeline crashed")
        finally:
            st.session_state.running = False

# ── Apply actions ─────────────────────────────────────────────────────────────
if apply_btn:
    conn = _ensure_db()
    all_results = get_all_results(conn)
    if not all_results:
        st.sidebar.warning("No results in database yet. Run a scan first.")
    else:
        all_records = [FileRecord(**r) for r in all_results]
        engine = ActionEngine(dry_run=dry_run)
        outcome = engine.apply_all(all_records, action_filter=action_filter)
        mode_label = "DRY RUN" if dry_run else "APPLIED"
        st.sidebar.success(
            f"[{mode_label}] {outcome['summary']}"
        )
        logger.info("Action results: %s", outcome["summary"])


# ── Results display ───────────────────────────────────────────────────────────
conn = _ensure_db()
all_db_results = get_all_results(conn)

if all_db_results:
    df = _results_to_df(all_db_results)

    # ── Summary metrics ──
    st.subheader("📊 Summary")
    actions = [r.get("suggested_action", "review") for r in all_db_results]
    col1, col2, col3, col4, col5 = st.columns(5)
    metrics = {
        "Total Files": len(all_db_results),
        "🗑️ Delete":   actions.count("delete"),
        "📦 Archive":  actions.count("archive"),
        "✅ Retain":   actions.count("retain"),
        "🔎 Review":   actions.count("review"),
    }
    for col, (label, value) in zip([col1, col2, col3, col4, col5], metrics.items()):
        with col:
            st.markdown(
                f'<div class="metric-card"><h2>{value}</h2><p>{label}</p></div>',
                unsafe_allow_html=True,
            )

    st.divider()

    # ── Filter & table ──
    st.subheader("📋 Results")
    filter_actions = st.multiselect(
        "Filter by Action",
        options=["delete", "archive", "retain", "review"],
        default=["delete", "archive", "retain", "review"],
        key="result_filter",
    )
    filtered = [r for r in all_db_results if r.get("suggested_action") in filter_actions]
    display_df = _results_to_df(filtered)

    if not display_df.empty:
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "display_path":    st.column_config.TextColumn("File", width="large"),
                "suggested_action": st.column_config.TextColumn("Action"),
                "retention_score": st.column_config.ProgressColumn(
                    "Score", min_value=0, max_value=100, format="%d"
                ),
                "confidence":      st.column_config.NumberColumn("Confidence", format="%.2f"),
                "category":        st.column_config.TextColumn("Category"),
                "file_size":       st.column_config.TextColumn("Size"),
                "last_modified":   st.column_config.TextColumn("Modified"),
                "reasoning":       st.column_config.TextColumn("Reasoning", width="large"),
                "processed_at":    st.column_config.TextColumn("Processed At"),
            },
        )
    else:
        st.info("No results match the selected filters.")

    st.divider()

    # ── Export ──
    st.subheader("⬇️ Export Report")
    report_data = json.dumps(all_db_results, indent=2, default=str)
    st.download_button(
        label="📥 Download JSON Report",
        data=report_data,
        file_name="retention_report.json",
        mime="application/json",
        use_container_width=True,
    )

else:
    # Empty state
    st.info(
        "👈 **Enter a folder path in the sidebar and click 'Scan & Analyze'** to begin.\n\n"
        "Supported file types: `jpg`, `jpeg`, `png`, `pdf`"
    )

# ── Logs expander ─────────────────────────────────────────────────────────────
if st.session_state.logs:
    with st.expander("📜 Session Logs", expanded=False):
        st.code("\n".join(st.session_state.logs), language="")
