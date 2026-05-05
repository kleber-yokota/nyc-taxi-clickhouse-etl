# extract — Details

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

Downloads TLC parquet files according to the specified filters.

**Args:**
- `data_dir`: Base directory for storing data. Defaults to `"data"`.
- `types`: Data types to download. Defaults to all types (fhv, fhvhv, green, yellow).
- `from_year`: Starting year (inclusive). Defaults to 2009.
- `to_year`: Ending year (inclusive). Defaults to current year.
- `mode`: `"incremental"` (skip downloaded) or `"full"` (reset state).
- `max_entries`: Optional limit on entries for testing.

**Returns:**
- `dict[str, int]` with keys: `downloaded`, `skipped`, `failed`, `total`.

**Side effects:**
- Creates `data/.download_state.json` with checksums
- Creates `data/errors/download_errors.log` with error entries
- Downloads files to `data/{type}/{type}_tripdata_{year}-{month}.parquet`
- Creates `data/known_missing.txt` with 404 URLs

---

### `extract.core.catalog.Catalog`

```python
class Catalog:
    def __init__(
        self,
        types: list[str] | None = None,
        from_year: int | None = None,
        to_year: int | None = None,
        max_entries: int | None = None,
    ) -> None:
    def generate(self) -> list[CatalogEntry]:
    def count(self) -> int:
```

Generates ordered list of available TLC data entries.

**Ordering:** Types in alphabetical order, months in chronological order within each type.

---

### `extract.core.state_manager.State`

```python
class State:
    def __init__(self, state_path: str | Path) -> None:
    def save(self, url: str, checksum: str) -> None:
    def get_checksum(self, url: str) -> str | None:
    def is_downloaded(self, url: str) -> bool:
    def reset(self) -> None:
    def log_error(self, url: str, error_type: ErrorType, detail: str = "") -> None:
```

Manages download state (checksums) and error logging.

---

### `extract.core.known_missing.KnownMissing`

```python
class KnownMissing:
    def __init__(self, path: str | Path) -> None:
    def is_missing(self, url: str) -> bool:
    def add(self, url: str) -> None:
```

Tracks URLs that returned HTTP 404 to avoid re-downloading.

---

### `extract.core.state.ErrorType`

```python
class ErrorType(Enum):
    MISSING_FILE = "missing_file"      # HTTP 404
    NETWORK_ERROR = "network_error"    # Connection timeout, DNS failure
    HTTP_ERROR = "http_error"          # HTTP 5xx
    CHECKSUM_MISMATCH = "checksum_mismatch"
    CORRUPT_FILE = "corrupt_file"
    UNKNOWN = "unknown"
```

---

## Models / Types

### `extract.core.catalog.CatalogEntry`

```python
@dataclass(frozen=True)
class CatalogEntry:
    data_type: str      # One of: "yellow", "green", "fhv", "fhvhv"
    year: int           # Year (e.g. 2024)
    month: int          # Month (1-12)

    @property
    def url(self) -> str:
        # Full CDN URL

    @property
    def filename(self) -> str:
        # {type}_tripdata_{year}-{month}.parquet

    @property
    def target_dir(self) -> str:
        # data_type (same as entry.data_type)
```

---

## Expected behavior (test cases)

### Happy path
- `run()` downloads all entries for a given type/year range
- Checksums are saved to state file after each successful download
- Existing files with matching checksums are skipped

### Invalid input
- `from_year > to_year` produces empty catalog (0 entries)
- Invalid `mode` value falls through to incremental behavior

### Infrastructure failure
- HTTP 404: URL added to known_missing.txt, download continues
- HTTP 5xx: Error logged, download continues
- Network error: Error logged with exception details, download continues
- KeyboardInterrupt: Temporary files cleaned up, graceful exit

---

## Edge cases and gotchas

### fhvhv availability
- Not available before 2016 (all months filtered out)
- Not available in future months beyond current date

### Interrupted downloads
- Temporary files use `.download.tmp` suffix
- On SIGINT/SIGTERM, tmp files are automatically removed
- State is only updated after successful download + checksum verification

### File updates
- If a file exists locally but content changed (checksum mismatch), old file is backed up to `.old`
- New file is downloaded and state is updated

### Missing files
- HTTP 404 responses are logged and the download continues (does not crash)
- Error is written to `data/errors/download_errors.log` in JSON Lines format

---

## Design decisions

| Decision | Alternative | Rationale |
|---|---|---|
| One class per file | Multiple classes in one file | REVIEW.md rule, cleaner separation of concerns |
| SHA-256 instead of MD5 | MD5 | Collision resistance, future-proof |
| Atomic rename (tmp → final) | In-place write | Prevents corrupted partial files |
| Continue on error | Stop on first error | Downloading 300+ files — one missing shouldn't stop the run |
| JSON Lines for error log | Single JSON array | Append-friendly, parseable line-by-line |
| requests instead of httpx | httpx | Simpler dependency, sufficient for download use case |
| Protocol for test fakes | Mock objects | Easier to verify state, no mock chains |

---

## Test structure

```
extract/tests/
├── conftest.py            # Shared fixtures: tmp_path, download_dir, existing_state
├── test_catalog.py        # CatalogEntry and Catalog tests
├── test_catalog_sha256.py # compute_sha256 function tests
├── test_downloader.py     # run() integration tests
├── test_downloader_download_full.py       # download_and_verify and handle_download_error
├── test_downloader_download_log_http.py   # _log_http_error and _fetch_content
├── test_downloader_download_verify_branches.py # download_and_verify branches
├── test_downloader_download_error_handling.py # Error handling scenarios
├── test_downloader_helpers.py             # Helper function tests (part 1)
├── test_downloader_helpers_utils.py       # Helper function tests (part 2)
├── test_downloader_io.py                  # File I/O operations
├── test_downloader_network.py             # Network error scenarios
├── test_downloader_ops.py                 # Download operations
├── test_downloader_ops_full.py            # Download operations (full)
├── test_downloader_ops_process.py         # process_entry tests
├── test_downloader_run.py                 # run() default mode and paths
├── test_downloader_run_integration.py     # run() full integration
├── test_downloader_run_loop.py            # run() counter values and result structure
├── test_downloader_run_params.py          # run() parameter passing
├── test_downloader_run_params_filters.py  # run() filters and mode behavior
├── test_downloader_run_full.py            # run() function tests
├── test_downloader_run_full_integration.py # run() integration tests
├── test_downloader_utils.py               # Utility function tests
├── test_e2e.py                            # E2E download flow
├── test_e2e_errors.py                     # E2E error handling
├── test_e2e_retry.py                      # E2E retry behavior
├── test_fuzz.py                           # Fuzz tests (atheris)
├── test_helpers.py                        # Downloader helper tests (part 1)
├── test_helpers_utils.py                  # Downloader helper tests (part 2)
├── test_interrupt.py                      # InterruptibleDownload tests
├── test_interrupt_edge_cases.py           # Interrupt cleanup edge cases
├── test_known_missing.py                  # KnownMissing tests
├── test_mutant_download_and_verify.py     # Mutation killing tests
├── test_mutant_killing.py                 # Mutation killing tests (core)
├── test_mutant_survivors.py               # Surviving mutant tests (part 1)
├── test_mutant_survivors_extra.py         # Surviving mutant tests (part 2)
├── test_properties.py                     # Property-based tests
├── test_state_manager.py                  # State persistence and loading
└── test_state_manager_extra.py            # State error logging and reset
```

---

## Logs and observability

| Event | Level | Module |
|---|---|---|
| Download start | INFO | `extract.downloader.downloader` |
| Download complete | INFO | `extract.downloader.downloader` |
| Known missing skip | INFO | `extract.downloader.downloader_ops` |
| Validation failure | WARNING | `extract.downloader.downloader_download` |
| Network error | ERROR | `extract.downloader.downloader_download` |
| HTTP error | ERROR | `extract.downloader.downloader_download` |
| Interrupt signal | INFO | `extract.core.interrupt` |
| Cleanup started | INFO | `extract.core.interrupt` |

---

## Relevant change history

| Date | Change | Why |
|---|---|---|
| 2025-05-05 | Split downloader.py into downloader.py + downloader_actions.py | Meet ≤150 LoC limit (§1) |
| 2025-05-05 | Split test files >200 lines into multiple files | Meet ≤200 LoC limit (§1) |
| 2025-05-05 | Renamed generic variables (data→state_data, result→download_result) | Meet naming rules (§5) |
