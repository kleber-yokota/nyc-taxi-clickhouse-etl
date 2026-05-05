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
from extract.downloader.downloader import run

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
│   ├── state.py               # Constants, ErrorType, compute_sha256
│   ├── state_manager.py       # State class (checksums, error logging)
│   ├── interrupt.py           # InterruptibleDownload (signal handling, cleanup)
│   ├── catalog.py             # Catalog + CatalogEntry (URL generation, ordering)
│   └── known_missing.py       # KnownMissing (404 URL tracker)
├── downloader/
│   ├── __init__.py
│   ├── downloader.py          # run() — main download orchestration (≤137 lines)
│   ├── downloader_actions.py  # Helper actions: apply_mode, log_download_complete, make_result
│   ├── downloader_download.py # download_and_verify, handle_download_error, _fetch_content
│   ├── downloader_ops.py      # process_entry, should_skip_download
│   └── downloader_util.py     # backup_existing_file, cleanup_stale_tmp, safe_unlink, HTTP error handlers
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
│   ├── test_catalog_sha256.py
│   ├── test_downloader.py
│   ├── test_downloader_download_full.py
│   ├── test_downloader_download_log_http.py
│   ├── test_downloader_download_verify_branches.py
│   ├── test_downloader_download_error_handling.py
│   ├── test_downloader_helpers.py
│   ├── test_downloader_io.py
│   ├── test_downloader_network.py
│   ├── test_downloader_ops.py
│   ├── test_downloader_ops_full.py
│   ├── test_downloader_ops_process.py
│   ├── test_downloader_run.py
│   ├── test_downloader_run_integration.py
│   ├── test_downloader_run_loop.py
│   ├── test_downloader_run_params.py
│   ├── test_downloader_run_params_filters.py
│   ├── test_downloader_run_full.py
│   ├── test_downloader_run_full_integration.py
│   ├── test_downloader_utils.py
│   ├── test_e2e.py            # E2E tests — download flow
│   ├── test_e2e_errors.py     # E2E tests — error handling
│   ├── test_e2e_retry.py
│   ├── test_fuzz.py
│   ├── test_helpers.py
│   ├── test_helpers_utils.py
│   ├── test_interrupt.py
│   ├── test_interrupt_edge_cases.py
│   ├── test_known_missing.py
│   ├── test_mutant_download_and_verify.py
│   ├── test_mutant_killing.py
│   ├── test_mutant_survivors.py
│   ├── test_mutant_survivors_extra.py
│   ├── test_properties.py
│   ├── test_state_manager.py
│   └── test_state_manager_extra.py
├── OVERVIEW.md
└── DETAILS.md
```

---

## Inputs and outputs

| Input | Type | Output | Type |
|---|---|---|---|
| `data_dir` | `str \| Path \| None` | `result` dict | `{"downloaded": int, "skipped": int, "failed": int, "total": int}` |
| `types` | `list[str] \| None` | state file | `data/.download_state.json` |
| `from_year` | `int \| None` | error log | `data/errors/download_errors.log` |
| `to_year` | `int \| None` | known_missing file | `data/known_missing.txt` |
| `mode` | `"incremental" \| "full"` | downloaded files | `data/{type}/{type}_tripdata_{year}-{month}.parquet` |

---

## External dependencies

- `requests` — HTTP client for downloading parquet files
- `pydantic` — Input validation (State class)

---

## Possible states / Lifecycle

```
START → CHECK_STATE → DOWNLOAD → VERIFY_CHECKSUM → SAVE_STATE
                                   ↘ ERROR → LOG_ERROR → CONTINUE/NEXT
                               ↘ INTERRUPT → CLEANUP_TMP → EXIT
```

---

## Metric limits in effect (§1 of AGENTS.md)

| Metric | Limit | Notes |
|---|---|---|
| Function LoC | ≤ 15 | All functions audited |
| Cyclomatic Complexity | < 10 | Max observed: 9 |
| File length (source) | ≤ 150 | downloader.py: 137, others < 100 |
| File length (test) | ≤ 200 | All test files split and verified |
| Mutation score | ≥ 85% | Currently 88.4% |
| Test coverage | ≥ 85% | Enforced via coverage report |

---

## What is NOT in this module

- Transform logic → see `transform/`
- Loading to ClickHouse/Garage → see `load/`
- Configuration management → see `config.py`
- CLI interface → currently a simple function call from `main.py`

---

## Files to read if you need more context

| File | Why read it |
|---|---|
| `DETAILS.md` | Function signatures, edge cases, design decisions |
| `tests/test_e2e.py` | Full download scenarios with VCR and responses mocks |
| `tests/test_properties.py` | Property-based tests for URL format and ordering |
