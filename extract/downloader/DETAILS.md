# extract/downloader — Details

---

## Public API

### `extract.downloader.downloader.run()`

```python
def run(
    data_dir: str | Path | None = None,
    types: list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    mode: str = "incremental",
    max_entries: int | None = None,
) -> dict[str, int]:
```

**Args:**
- `data_dir`: Base directory for storing data. Defaults to `"data"`.
- `types`: Data types to download. Defaults to all types.
- `from_year`: Starting year (inclusive). Defaults to 2009.
- `to_year`: Ending year (inclusive). Defaults to current year.
- `mode`: `"incremental"` or `"full"`.
- `max_entries`: Optional limit for testing.

**Returns:** `{"downloaded": int, "skipped": int, "failed": int, "total": int}`

**Side effects:** Downloads files, creates state file, error log, known_missing file.

---

### `extract.downloader.downloader_download.download_and_verify()`

```python
def download_and_verify(
    entry: CatalogEntry,
    data_dir: Path,
    state: State,
    known_missing: KnownMissing | None = None,
) -> str:
```

Downloads a file and verifies its SHA-256 checksum.

**Returns:** `"downloaded"`, `"skipped"`, or `"failed"`.

---

### `extract.downloader.downloader_ops.process_entry()`

```python
def process_entry(
    entry: CatalogEntry,
    data_dir: Path,
    state: State,
    known_missing: KnownMissing,
    downloaded: int,
    skipped: int,
    failed: int,
) -> tuple[int, int, int]:
```

Processes a single catalog entry, updating counters.

**Returns:** Updated `(downloaded, skipped, failed)` tuple.

---

### `extract.downloader.downloader_ops.should_skip_download()`

```python
def should_skip_download(
    entry: CatalogEntry,
    state: State,
    known_missing: KnownMissing,
    data_dir: Path,
) -> bool:
```

Checks if a download should be skipped.

**Returns:** `True` if the download should be skipped.

---

## Models / Types

- `CatalogEntry` — from `extract.core.state`
- `State` — from `extract.core.state_manager`
- `KnownMissing` — from `extract.core.known_missing`
- `ErrorType` — from `extract.core.state`

---

## Expected behavior (test cases)

### Happy path
- `download_and_verify()` downloads file, verifies checksum, saves state
- `process_entry()` updates counters correctly
- `should_skip_download()` returns True when file already exists with matching checksum

### Invalid input
- Missing file (404): returns `"failed"`, logs error, adds to known_missing
- Network error: returns `"failed"`, logs error, continues

### Infrastructure failure
- HTTP 5xx: Error logged, download continues
- KeyboardInterrupt: Temp files cleaned up, graceful exit

---

## Edge cases and gotchas

### Checksum mismatch
- Old file backed up to `.old` suffix
- New file downloaded and state updated

### Interrupted downloads
- Temp files use `.download.tmp` suffix
- Cleanup on interrupt removes temp files

### Known missing URLs
- 404 URLs added to known_missing.txt
- Subsequent runs skip these URLs

---

## Design decisions

| Decision | Alternative | Rationale |
|---|---|---|
| Separate downloader_actions.py | Keep all helpers in downloader.py | Meet ≤150 LoC limit |
| Protocol for test fakes | Mock objects | Easier to verify state |
| Continue on error | Stop on first error | 300+ files — one missing shouldn't stop |
| JSON Lines for error log | Single JSON | Append-friendly |

---

## Test structure

```
extract/tests/downloaders/
├── test_downloader_download_full.py         # download_and_verify + handle_download_error
├── test_downloader_download_log_http.py     # _log_http_error + _fetch_content
├── test_downloader_download_verify_branches.py # download_and_verify branches
├── test_downloader_download_error_handling.py # Error handling
├── test_downloader_helpers.py               # Helper tests (part 1)
├── test_downloader_helpers_utils.py         # Helper tests (part 2)
├── test_downloader_io.py                    # File I/O
├── test_downloader_network.py               # Network errors
├── test_downloader_ops.py                   # Download operations
├── test_downloader_ops_full.py              # Operations (full)
├── test_downloader_ops_process.py           # process_entry tests
├── test_downloader_run.py                   # run() default mode
├── test_downloader_run_integration.py       # run() integration
├── test_downloader_run_loop.py              # run() counters
├── test_downloader_run_params.py            # run() parameters
├── test_downloader_run_params_filters.py    # run() filters
├── test_downloader_run_full.py              # run() function
├── test_downloader_run_full_integration.py  # run() integration
├── test_downloader_utils.py                 # Utility functions
├── test_e2e.py                              # E2E download flow
├── test_e2e_errors.py                       # E2E error handling
├── test_e2e_retry.py                        # E2E retry behavior
├── test_fuzz.py                             # Fuzz tests
├── test_helpers.py                          # Helper tests (part 1)
├── test_helpers_utils.py                    # Helper tests (part 2)
├── test_mutant_download_and_verify.py       # Mutation killing
├── test_mutant_killing.py                   # Mutation killing (core)
├── test_mutant_survivors.py                 # Survivor tests (part 1)
├── test_mutant_survivors_extra.py           # Survivor tests (part 2)
└── conftest.py                              # Shared fixtures
```

---

## Logs and observability

| Event | Level | Module |
|---|---|---|
| Download start | INFO | `extract.downloader.downloader` |
| Download complete | INFO | `extract.downloader.downloader` |
| Known missing skip | INFO | `extract.downloader.downloader_ops` |
| Network error | ERROR | `extract.downloader.downloader_download` |
| HTTP error | ERROR | `extract.downloader.downloader_download` |

---

## Relevant change history

| Date | Change | Why |
|---|---|---|
| 2025-05-05 | Split downloader.py → downloader.py + downloader_actions.py | Meet ≤150 LoC limit (§1) |
| 2025-05-05 | Renamed generic variables | Meet naming rules (§5) |
