# Module: Extract

## TODO (Managed by Maintainer)

### Draft
- [x] Fix critical bug: add `ErrorType` import in `state_manager.py`
- [x] Fix type hints: `state: object` -> `State`, `known_missing: object` -> `KnownMissing`
- [x] Fix type hints: `entries: list` -> `list[CatalogEntry]`, `e: Exception` -> `HTTPError`
- [x] Add `__all__` declarations to all modules

### Refine
- [x] Remove 8 test compatibility aliases in `downloader.py:127-134`
- [x] Consolidate duplicate HTTP error handling (`_log_http_error` + `handle_http_error`)
- [x] Fill empty `__init__.py` files with public API exports
- [x] Replace `target_dir` with `data_type` in `is_pushed_in_manifest` calls
- [x] Remove extra blank lines in `ops.py:18-20`
- [x] Enforce 15/10/3 rules (function length, complexity, nesting)

### Test
- [x] Merge `test_interrupt_signal.py` into `test_interrupt.py`
- [x] Consolidate `test_state.py` + `test_catalog_sha256.py` (duplicate `compute_sha256` tests)
- [x] Run mutation testing (mutmut) and verify all mutants killed
- [x] Run E2E tests with VCR cassettes
- [x] Run fuzz and property-based tests

### Audit
- [x] Resolve signal handler conflict (global `signal.signal` vs `KeyboardInterrupt`)
- [x] Consolidate signal handlers in `interrupt.py` (singleton or module-level)
- [x] Fix `except Exception` re-raise without context in `download.py:70-73`
- [x] Verify coverage >= 85%
- [x] Verify Radon CC < 10, Radon MI > 70
- [x] Verify Xenon gates pass
- [x] Verify Vulture finds no dead code
- [x] Verify LCOM <= 2 for all classes

---

## Overview

Module `extract/` is an ETL extraction system for downloading NYC TLC trip data parquet files from AWS CloudFront CDN.

**Statistics:**
- 935 lines of source code across 14 files (2 packages: `core/`, `downloader/`) — files renamed: `downloader_actions.py`→`actions.py`, `downloader_download.py`→`download.py`, `downloader_ops.py`→`ops.py`, `downloader_util.py`→`utils.py`
- ~4500 lines of tests across 49 files
- 5 VCR cassettes for reproducible HTTP testing
- External dependencies: `requests`, `hashlib`, `json`, `signal`, `logging`

**Architecture:**
```
extract/
  core/                  <-- Domain logic (pure business logic)
    state.py             -- Constants, enums, CatalogEntry, utilities
    catalog.py           -- Catalog generator for parquet files
    state_manager.py     -- State persistence and error logging
    interrupt.py         -- Signal handling and cleanup
    push_manifest.py     -- S3 push manifest reader
    known_missing.py     -- 404 URL tracking

  downloader/            <-- Orchestration (coordinates I/O)
    downloader.py                -- Main entry point (run())
    actions.py                   -- Helper actions
    download.py                  -- Download + checksum verification
    ops.py                       -- Entry processing, skip logic
    utils.py                     -- File utilities (backup, unlink)
```

**Main flow (`run()`):**
1. Resolve data directory
2. Generate Catalog (list of CatalogEntry)
3. Load State (saved checksums)
4. Apply mode (incremental or full reset)
5. Load KnownMissing (404 URLs)
6. Load PushManifest (S3 upload tracking)
7. Loop: for each entry -> skip check -> download -> verify -> state save

**Expected behavior:**
- `run()` downloads all parquet files not yet in state
- Skips files already downloaded (checksum match) or already in S3
- Records 404 URLs to skip on future runs
- Handles interruptions gracefully (cleanups temp files)
- Returns result dict: `{"downloaded": N, "skipped": N, "failed": N, "total": N}`

---

## Details

### Tech Stack
- Python 3.11+
- requests (HTTP client for downloads)
- hashlib (SHA-256 checksums for integrity verification)
- json (state and manifest serialization)
- signal (interrupt handling for graceful shutdown)
- logging (structured logging throughout)

### Toolchain
- `uv run` for all command execution
- `pytest` for unit and integration tests
- `mutmut` for mutation testing
- `hypothesis` for property-based and fuzz testing
- `vcrpy` for HTTP cassette recording/playback
- `coverage` for code coverage analysis
- `radon` for cyclomatic complexity and maintainability index
- `xenon` for quality gates
- `vulture` for dead code detection
- `cohesion` for class cohesion analysis

### Quality Gates (CI)
- Coverage >= 85%
- Radon CC average < 10
- Radon MI average > 70
- Xenon: max-absolute B, max-modules B, max-average A
- Vulture: no dead code
- LCOM <= 2 per class
- Cohesion: pass

### Architectural Guardrails (15/10/3)
- Functions: <= 15 lines
- Cyclomatic Complexity: < 10
- Nesting depth: <= 3 levels

### Module Dependencies
```
core/state.py          --> stdlib (hashlib, dataclasses, enum, datetime)
core/catalog.py        --> core/state
core/state_manager.py  --> core/state
core/interrupt.py      --> core/state
core/known_missing.py  --> core/state
core/push_manifest.py  --> stdlib (json, logging)
downloader/actions.py        --> stdlib
 downloader/utils.py          --> core/state, core/state_manager
 downloader/download.py       --> utils, core/known_missing, core/state, core/state_manager
 downloader/ops.py            --> core/*, download
 downloader/downloader.py     --> core/*, actions, download, ops, utils
```

### Public API
- `extract.downloader.downloader.run()` — main entry point
- `extract.core.catalog.Catalog` — catalog generator
- `extract.core.state_manager.State` — state manager
- `extract.core.known_missing.KnownMissing` — 404 URL tracker
- `extract.core.push_manifest.load_push_manifest()` — manifest loader
- `extract.core.state.CatalogEntry` — data model for parquet files

### Known Issues

**Critical:**
- ~~`state_manager.py:82` — Uses `ErrorType` without importing from `.state` (causes NameError at runtime)~~ (RESOLVED: added `from .state import ErrorType`)

**Moderate:**
- ~~`downloader.py:127-134` — 8 test compatibility aliases (dead code/workaround)~~ (RESOLVED: removed)
- ~~`downloader_actions.py:23` — `state: object` instead of `State`~~ (RESOLVED: `state: State`)
- ~~`downloader_download.py:80` — `known_missing: object` instead of `KnownMissing`~~ (RESOLVED: `known_missing: KnownMissing`)
- ~~`downloader_util.py:47` — `e: Exception` instead of `requests.HTTPError`~~ (RESOLVED: `e: requests.HTTPError`)
- ~~`downloader_download.py:79` — `entries: list` without type parameter~~ (RESOLVED: `entries: list[CatalogEntry]`)
- ~~`downloader_download.py:107` + `downloader_util.py:47` — Duplicate HTTP error handling logic~~ (RESOLVED: consolidated into `_log_http_error`)
- ~~`interrupt.py:31-32` — `signal.signal` re-registered on every instantiation~~ (RESOLVED: module-level singleton with `_ensure_signal_handler()`)
- ~~`downloader.py:109-110` — `KeyboardInterrupt` handler conflicts with InterruptibleDownload~~ (RESOLVED: removed conflicting handler, signal handled at module level)
- ~~`downloader_download.py:70-73` — `except Exception` re-raises without context~~ (RESOLVED: catches and returns "failed" with logging)
- ~~`downloader_ops.py:18-20` — 3 extra blank lines~~ (RESOLVED: removed)
- ~~`test_interrupt_signal.py` — Duplicate of test in `test_interrupt.py`~~ (RESOLVED: merged)
- ~~`test_state.py` + `test_catalog_sha256.py` — Both test `compute_sha256`~~ (RESOLVED: consolidated)

**Minor:**
- ~~All `__init__.py` files are empty (no public API exports)~~ (RESOLVED: all have `__all__` and exports)
- ~~No `__all__` declarations in any module~~ (RESOLVED: added to all modules)
- ~~`downloader_ops.py:50` — `target_dir` passed where `data_type` was expected~~ (RESOLVED: replaced with `data_type`)
