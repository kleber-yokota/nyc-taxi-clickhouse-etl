# extract — Details

---

## Public API

### `extract.core.downloader.run()`

```python
def run(
    data_dir: str | Path | None = None,
    types: list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    mode: str = "incremental",
) -> dict[str, int]:
```

Downloads TLC parquet files according to the specified filters.

**Args:**
- `data_dir`: Base directory for storing data. Defaults to `"data"`.
- `types`: Data types to download. Defaults to all types (fhv, fhvhv, green, yellow).
- `from_year`: Starting year (inclusive). Defaults to 2009.
- `to_year`: Ending year (inclusive). Defaults to current year.
- `mode`: `"incremental"` (skip downloaded) or `"full"` (reset state).

**Returns:**
- `dict[str, int]` with keys: `downloaded`, `skipped`, `failed`, `total`.

**Side effects:**
- Creates `data/.download_state.json` with checksums
- Creates `data/errors/download_errors.log` with error entries
- Downloads files to `data/{type}/{type}_tripdata_{year}-{month}.parquet`

---

### `extract.core.catalog.Catalog`

```python
class Catalog:
    def __init__(
        self,
        types: list[str] | None = None,
        from_year: int | None = None,
        to_year: int | None = None,
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
    def __init__(
        self,
        state_path: str | Path | None = None,
        errors_dir: str | Path | None = None,
    ) -> None:
    def save(self, url: str, checksum: str) -> None:
    def get_checksum(self, url: str) -> str | None:
    def is_downloaded(self, url: str) -> bool:
    def reset(self) -> None:
    def log_error(self, url: str, error_type: ErrorType, detail: str = "") -> None:
```

Manages download state (checksums) and error logging.

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

## Edge cases

### fhvhv availability
- Not available before 2016 (all months filtered out)
- Not available in 2026-02 and 2026-03

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

| Decision | Rationale |
|---|---|
| One class per file | REVIEW.md rule, cleaner separation of concerns |
| SHA-256 instead of MD5 | Collision resistance, future-proof |
| Atomic rename (tmp → final) | Prevents corrupted partial files |
| Continue on error | Downloading 300+ files — one missing shouldn't stop the run |
| JSON Lines for error log | Append-friendly, parseable line-by-line |
| No pydantic dependency | State management is simple enough for dataclasses |

---

## What is NOT in this module

- Actual data transformation → see `transform/`
- Database loading → see `load/`
- CLI interface → currently a simple function call

---

## Files to read if you need more context

| File | Why read it |
|---|---|
| `OVERVIEW.md` | Module flow and structure |
| `tests/test_e2e.py` | Download flow scenarios with VCR cassettes |
| `tests/test_e2e_errors.py` | Error handling scenarios with VCR cassettes |
| `tests/test_e2e_retry.py` | Retry/skip behavior with VCR |
| `tests/test_properties.py` | Property tests for ordering and URL format |
| `tests/test_fuzz.py` | Fuzz tests for state serialization |
