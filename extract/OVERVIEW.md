# extract — Overview

> **For the agent**: Read this file first. It describes the module's general flow.
> If you need implementation details, refer to `DETAILS.md`.

---

## What this module does

`extract` is responsible for **downloading NYC TLC trip record parquet files** from the public CDN into the local `data/` directory.

---

## Main flow

```
Catalog.generate() → ordered list of (type, year, month) entries
  → for each entry:
    → check state (already downloaded?)
    → download via HTTP with retry/interrupt handling
    → verify SHA-256 checksum
    → backup old file if content changed
    → persist state + log errors
```

### Concrete example

```python
from extract.core.downloader import run

result = run(data_dir="data", types=["yellow"], from_year=2024, to_year=2024, mode="incremental")
# result: {"downloaded": 12, "skipped": 0, "failed": 0, "total": 12}
```

---

## File structure

```
extract/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── state.py               # Constants, ErrorType, utilities
│   ├── state_manager.py       # State class (checksums, error logging)
│   ├── interrupt.py           # InterruptibleDownload (signal handling, cleanup)
│   ├── catalog.py             # Catalog + CatalogEntry (URL generation, ordering)
│   └── downloader.py          # run() — main download orchestration
├── tests/
│   ├── __init__.py
│   ├── conftest.py            # Shared fixtures + VCR config
│   ├── cassettes/             # VCR cassettes for e2e tests
│   │   ├── success_200.yaml   # 3 x 200 OK
│   │   ├── partial_200_404.yaml # mixed 200/404
│   │   ├── all_404.yaml       # 12 x 404 Not Found
│   │   ├── all_500.yaml       # 12 x 500 Internal Server Error
│   │   └── checksum_mismatch.yaml # 12 x 200 OK (different content)
│   ├── test_catalog.py
│   ├── test_downloader.py
│   ├── test_e2e.py            # E2E tests — download flow
│   ├── test_e2e_errors.py     # E2E tests — error handling
│   ├── test_e2e_retry.py
│   ├── test_properties.py
│   ├── test_fuzz.py
│   ├── test_interrupt.py
│   └── test_known_missing.py
├── OVERVIEW.md
└── DETAILS.md
```

---

## Inputs and outputs

| Input | Type | Output | Type |
|---|---|---|---|
| `data_dir` | `Path` | `result` dict | `{"downloaded": int, "skipped": int, "failed": int, "total": int}` |
| `types` | `list[str]` | state file | `data/.download_state.json` |
| `mode` | `"incremental" | "full"` | error log | `data/errors/download_errors.log` |

---

## External dependencies

- `httpx` — HTTP client for downloading parquet files

---

## Possible states / Lifecycle

```
START → CHECK_STATE → DOWNLOAD → VERIFY_CHECKSUM → SAVE_STATE
                                   ↘ ERROR → LOG_ERROR → CONTINUE/NEXT
                               ↘ INTERRUPT → CLEANUP_TMP → EXIT
```

---

## What is NOT in this module

- Transform logic → see `transform/`
- Loading to ClickHouse/Garage → see `load/`
- Configuration management → see `config.py`

---

## Files to read if you need more context

| File | Why read it |
|---|---|
| `DETAILS.md` | Function signatures, edge cases, design decisions |
| `tests/test_e2e.py` | Full download scenarios with VCR and responses mocks |
| `tests/test_properties.py` | Property-based tests for URL format and ordering |
