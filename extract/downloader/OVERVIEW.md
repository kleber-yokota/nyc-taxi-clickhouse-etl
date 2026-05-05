# extract/downloader — Overview

> **For the agent**: This module handles the download orchestration for TLC parquet files.

---

## What this module does

Orchestrates downloading NYC TLC trip record parquet files from the public CDN, verifying checksums, managing state, and handling errors/interrupts.

---

## Main flow

```
run() → Catalog.generate() → State() → KnownMissing()
  → for each entry:
    → should_skip_download() → check state + known_missing
    → download_and_verify() → HTTP fetch → checksum verify → save state
    → process_entry() → update counters
  → log_download_complete() → return result dict
```

### Concrete example

```python
from extract.downloader.downloader import run

result = run(types=["yellow"], from_year=2024, to_year=2024, mode="incremental")
# result: {"downloaded": 12, "skipped": 3, "failed": 1, "total": 16}
```

---

## File structure

```
extract/downloader/
├── __init__.py
├── downloader.py              # run() — main orchestration (137 lines)
├── downloader_actions.py      # Helper actions: apply_mode, log_download_complete, make_result (60 lines)
├── downloader_download.py     # download_and_verify, handle_download_error, _fetch_content (139 lines)
├── downloader_ops.py          # process_entry, should_skip_download (89 lines)
└── downloader_util.py         # backup_existing_file, cleanup_stale_tmp, safe_unlink, HTTP error handlers
```

**Metric limits:**
- `downloader.py`: 137 lines (≤150)
- `downloader_download.py`: 139 lines (≤150)
- All other files: < 100 lines

---

## Inputs and outputs

| Input | Type | Output | Type |
|---|---|---|---|
| `data_dir` | `str \| Path \| None` | Result dict | `{"downloaded": int, ...}` |
| `types` | `list[str] \| None` | State file | `.download_state.json` |
| `mode` | `"incremental" \| "full"` | Error log | `errors/download_errors.log` |

---

## External dependencies

- `requests` — HTTP client
- `extract.core.catalog` — Catalog + CatalogEntry
- `extract.core.state` — ErrorType, compute_sha256
- `extract.core.state_manager` — State
- `extract.core.known_missing` — KnownMissing
- `extract.core.interrupt` — InterruptibleDownload

---

## Possible states / Lifecycle

```
run() → INIT → CHECK_STATE → DOWNLOAD → VERIFY → SAVE_STATE → NEXT
                                   ↘ ERROR → LOG → CONTINUE
                               ↘ INTERRUPT → CLEANUP → EXIT
```

---

## What is NOT in this module

- Catalog generation → `extract/core/catalog.py`
- State management → `extract/core/state_manager.py`
- Signal handling → `extract/core/interrupt.py`
- Known missing tracking → `extract/core/known_missing.py`

---

## Files to read if you need more context

| File | Why read it |
|---|---|
| `downloader_download.py` | Download and verification logic |
| `downloader_ops.py` | Entry processing and skip logic |
| `downloader_util.py` | File utility functions |
