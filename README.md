# Bulk File Retention Analyzer

This is my saravam tool that recursively scans a folder, extracts text from documents via the **Sarvam Document Intelligence API**, classifies each file for retention using an **Saravam Chat completion**, and generates an actionable report —  from my fav tool:  Streamlit UI.

---

## I has these features 
- Recursive scan for `jpg`, `jpeg`, `png`, `pdf` files
- Text extraction via Sarvam Document Intelligence API
- Saravam LLM-based retention classification (delete / archive / retain / review)
- SQLite persistence with hash-based deduplication (skips unchanged files)
- Asynchronous processing with `asyncio` (configurable concurrency limit)
- Dry-run mode — preview actions without touching files
- Safe "delete" — files are moved to `.trash/`, never permanently deleted
- JSON report export
- I have also implemented exponential backoff retry on 429 / 5xx API errors :)

---

## This is my project structure

```
bulk_retention_analyzer/
├── app.py               # Streamlit UI
├── config.py            # Environment-based configuration
├── database.py          # SQLite layer
├── file_scanner.py      # Recursive file scanner
├── sarvam_client.py     # Sarvam Document Intelligence API client
├── llm_client.py        # Chat Completion API client
├── retention_engine.py  # Pipeline orchestrator
├── action_engine.py     # Safe file action executor
├── models.py            # Pydantic v2 data models
├── utils.py             # Shared helpers
├── requirements.txt
└── .env.example         # Configuration template
```

---

## how to use

### 1. Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Open .env and set your SARVAM_API_KEY
```

### 3. Run the app

```bash
python3 -m streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## You will need to update these following configurations

All settings are read from environment variables (or a `.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `SARVAM_API_KEY` | *(required)* | Your Sarvam API key |
| `SARVAM_DOC_ENDPOINT` | `https://api.sarvam.ai/v1/document-intelligence/extract` | Document extraction endpoint |
| `SARVAM_CHAT_ENDPOINT` | `https://api.sarvam.ai/v1/chat/completions` | Chat completion endpoint |
| `LLM_MODEL_NAME` | `sarvam-2b` | LLM model for classification |
| `DB_PATH` | `retention.db` | SQLite database file path |
| `MAX_CONCURRENCY` | `5` | Max concurrent API calls |
| `MAX_TEXT_CHARS` | `2000` | Max characters sent to LLM |
| `HTTP_TIMEOUT` | `60` | API request timeout (seconds) |
| `MAX_RETRIES` | `3` | Retry attempts for 429/5xx errors |

---

## LLM Output format - I coded in the prompt

Each file receives a structured decision:

```json
{
  "retention_score": 85,
  "category": "financial",
  "suggested_action": "retain",
  "confidence": 0.92,
  "reasoning": "Document contains invoice records subject to 7-year retention policy."
}
```

| Field | Values |
|-------|--------|
| `retention_score` | 0–100 |
| `category` | `legal` · `financial` · `operational` · `personal` · `ephemeral` · `unknown` |
| `suggested_action` | `delete` · `archive` · `retain` · `review` |
| `confidence` | 0.0–1.0 |

---

## How Actions Work - this is for your choice, you can create your own

| Action | Effect (Dry Run) | Effect (Apply) |
|--------|-----------------|----------------|
| `delete` | Preview `.trash/` destination | Move to `.trash/` beside original |
| `archive` | Preview `.archive/` destination | Move to `.archive/` beside original |
| `retain` | No-op | No-op |
| `review` | Flagged in log | Flagged in log |

> **Safety guarantee**: Files are never permanently deleted. "Delete" always moves to a `.trash` folder.

---

## Database table

```sql
CREATE TABLE files (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path        TEXT UNIQUE,
    file_hash        TEXT,
    file_size        INTEGER,
    last_modified    TEXT,
    extracted_text   TEXT,    -- first 500 chars only (security)
    retention_score  INTEGER,
    category         TEXT,
    suggested_action TEXT,
    confidence       REAL,
    reasoning        TEXT,
    processed_at     TEXT
);
```

