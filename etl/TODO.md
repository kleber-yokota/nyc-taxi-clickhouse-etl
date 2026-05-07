# ETL Module TODO

## Overview
ETL orchestrator coordinates extract → upload pipeline.

## Tasks

### 1. Checkpoint — ULID + float timestamps
- [x] Replace `uuid.uuid4().hex` with ULID for time-sortable `pipeline_id`
- [x] Replace `started_at`/`finished_at` datetime strings with `float` (time.time())
- [x] Remove unused `uuid` and `datetime` imports

### 2. Checksum — generic `compute()` method
- [x] Rename `compute_sha256()` → `compute()` for algorithm agnosticism
- [x] Update all callers and tests

### 3. ETLConfig — fix defaults vs docstring mismatch
- [x] `from_year`: default `2009` (was `None`, doc said 2009)
- [x] `to_year`: default `datetime.now().year` (was `None`, doc said current year)
- [x] Update tests to match actual defaults

### 4. Orchestrator — add `data_dir` and `state` attributes
- [x] Add `self.data_dir: Path | None` and `self.state: PipelineState | None`
- [x] Use assertions instead of passing `data_dir`/`state` as parameters
- [x] Update all internal methods to use `self.data_dir` and `self.state`
- [x] Update all tests

### 5. Break up Orchestrator — too many responsibilities
- [x] Extract `_execute_with_retry` → `RetryPolicy` class (`etl/retry.py`)
- [x] Extract checkpoint building → `CheckpointBuilder` class (`etl/checkpoint_builder.py`)
- [x] Extract manifest updating → `ManifestUpdater` class (`etl/manifest_updater.py`)
- [x] Keep only `run()` as public API on `Orchestrator`

### 6. Fix mid-function imports
- [ ] `checksum_impl.py:30` — `from upload.core.checksum import compute_sha256` — lazy import to avoid circular dep
- [ ] `orchestrator.py:103` — `from extract.downloader.downloader import run` — lazy import to avoid circular dep
- [ ] `orchestrator.py:118-119` — `from upload.core.runner import upload_from_env` + `UploadConfig` — lazy import to avoid circular dep
- [ ] `orchestrator.py:134` — `from .manifest import load_manifest` — CAN be moved to top-level (no circular dep)

### 7. Test cleanup
- [x] Update tests for checkpoint field types (ULID, float timestamps)
- [x] Update tests for `compute()` method rename
- [x] Update tests for Orchestrator refactoring
- [x] All 134 tests passing

### 8. Documentation
- [ ] Update `docs/modules/etl.md` for all API changes
- [ ] Update `docs/INDEX.md` with current status
