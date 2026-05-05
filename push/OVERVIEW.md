# push — Overview

> **For the agent**: Read this file first. It describes the module's general flow.
> If you need implementation details, refer to `DETAILS.md`.

---

## What this module does

`push` is responsible for **uploading NYC TLC trip record parquet files from local storage to an S3-compatible object store** (MinIO, AWS S3, etc.).

---

## Main flow

```
upload(data_dir, client, state, config) → walk data/ tree
  → for each *.parquet file:
    → compute SHA-256 checksum
    → check push state (already pushed with same checksum?)
    → if not pushed or overwrite=True:
      → multipart upload via S3Client
      → record push in state
    → return PushResult { uploaded, skipped, failed, total }
    → if delete_after_push=True: delete local file
```

### Concrete example

```python
from push.core import S3Client, PushState, UploadConfig, upload

client = S3Client.from_env(
    bucket="nyc-taxi-data",
    prefix="data",
    endpoint_url="http://localhost:9000",
)
state = PushState("data/.push_state.json")
config = UploadConfig(overwrite=False)

result = upload("data", client, state, config)
# result: PushResult(uploaded=12, skipped=0, failed=0, total=12)
```

### Environment-based upload

```python
from push.core import upload_from_env
from push.core.state import UploadConfig

# Requires: S3_BUCKET, S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
result = upload_from_env("data")
# result: PushResult(uploaded=12, skipped=0, failed=0, total=12)
```

### Upload with auto-delete

```python
from push.core import upload_from_env
from push.core.state import UploadConfig

# Upload and delete local files after successful push
config = UploadConfig(delete_after_push=True)
result = upload_from_env("data", config=config)
# result: PushResult(uploaded=12, skipped=0, failed=0, total=12)
# Local parquet files are deleted after upload confirmation
```

---

## File structure

```
push/
├── __init__.py               # Re-exports public API
├── core/
│   ├── __init__.py           # Re-exports public API
│   ├── client.py             # S3Client — DI wrapper around boto3
│   ├── ops.py                # Raw boto3 operations + S3Ops Protocol
│   ├── push.py               # upload() — orchestration
│   ├── runner.py             # upload_from_env() — env var entry point
│   ├── filter.py             # collect_files() — file collection + filtering
│   ├── checksum.py           # compute_sha256(), compute_content_type()
│   ├── state.py              # PushState, PushResult, UploadConfig
│   └── errors.py             # S3ClientError
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared fixtures + MinIO container
│   ├── fake_s3.py            # FakeS3Client — in-memory test double
│   ├── test_core_upload.py   # Unit tests for upload()
│   ├── test_core_env.py      # Unit tests for upload_from_env()
│   ├── test_filter.py        # Unit tests for file filtering
│   ├── test_checksum.py      # Unit tests for checksum utilities
│   ├── test_state.py         # Unit tests for state types
│   ├── test_e2e_client.py    # E2E tests — S3Client methods
│   ├── test_e2e_upload.py    # E2E tests — full upload pipeline
│   ├── test_properties.py    # Property-based tests (hypothesis)
│   └── test_fuzz.py          # Fuzz tests (atheris)
├── OVERVIEW.md
└── DETAILS.md
```

---

## Inputs and outputs

| Input | Type | Output | Type |
|---|---|---|---|
| `data_dir` | `Path` | `PushResult` | `uploaded, skipped, failed, total` |
| `client` | `S3Client` | state file | `.push_state.json` |
| `state` | `PushState` | S3 objects | `data/{type}/{filename}.parquet` |
| `config` | `UploadConfig` | — | — |

---

## External dependencies

- `boto3` — AWS SDK for S3 operations (works with MinIO via custom endpoint)
- `minio` — MinIO client (used in testcontainers E2E tests)

---

## Architecture

### Dependency Injection

`S3Client` accepts an `S3Ops`-compatible client via its constructor. Infrastructure clients are never instantiated inside methods — always injected. This enables testing with `FakeS3Client` or mocks.

```python
# Real client (injected from environment)
client = S3Client.from_env(bucket="my-bucket")

# Test client (injected fake)
client = S3Client(FakeS3Client(bucket="test"), bucket="test")
```

### S3Ops Protocol

`push/core/ops.py` defines `S3Ops` as a Protocol. All raw boto3 operation functions (`put_object`, `head_object`, etc.) accept `S3Ops` instead of concrete types. This decouples the push module from boto3.

---

## Possible states / Lifecycle

```
START → COLLECT_FILES → CHECK_PUSHED → UPLOAD → RECORD_STATE
                                  ↘ SKIP (already pushed)
                              ↘ ERROR → LOG_ERROR → CONTINUE
```

---

## What is NOT in this module

- Downloading files → see `extract/`
- Transform logic → see `transform/`
- Loading to ClickHouse/Garage → see `load/`
- CLI interface → currently a simple function call

---

## Files to read if you need more context

| File | Why read it |
|---|---|
| `DETAILS.md` | Function signatures, edge cases, design decisions |
| `tests/test_core_upload.py` | Full unit test scenarios |
| `tests/test_e2e_client.py` | MinIO integration tests for S3Client |
