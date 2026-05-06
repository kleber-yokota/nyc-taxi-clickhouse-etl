# push — Details

---

## Public API

### `push.core.upload()`

```python
def upload(
    data_dir: str | Path,
    client: S3Client,
    state: PushState,
    config: UploadConfig | None = None,
) -> PushResult:
```

Uploads files from `data_dir` to S3. Walks the directory tree, computes SHA-256 checksums, and uploads only files that haven't been pushed (or when `config.overwrite=True`).

**Args:**
- `data_dir`: Path to the local data directory.
- `client`: Configured S3Client instance.
- `state`: PushState instance for tracking uploads.
- `config`: Upload configuration (include/exclude patterns, overwrite flag).

**Returns:**
- `PushResult` with `uploaded`, `skipped`, `failed`, `total` counts.

---

### `push.core.upload_from_env()`

```python
def upload_from_env(
    data_dir: str | Path,
    config: UploadConfig | None = None,
    bucket: str | None = None,
    prefix: str | None = None,
    endpoint_url: str | None = None,
) -> PushResult:
```

Uploads files using environment variables for S3 configuration.

**Args:**
- `data_dir`: Path to the local data directory.
- `config`: Upload configuration (include/exclude/overwrite).
- `bucket`: S3 bucket name (overrides `S3_BUCKET` env var).
- `prefix`: S3 key prefix (overrides `S3_PREFIX` env var).
- `endpoint_url`: S3 endpoint URL (overrides `S3_ENDPOINT_URL` env var).

**Returns:**
- `PushResult` with `uploaded`, `skipped`, `failed`, `total` counts.

**Raises:**
- `ValueError`: If `S3_BUCKET` is not set in environment or as argument.
- `RuntimeError`: If AWS credentials are not available.

---

### `push.core.client.S3Client`

```python
class S3Client:
    def __init__(self, client: S3Ops, bucket: str, prefix: str = ..., part_size: int = ...) -> None:
    @classmethod
    def from_env(cls, bucket: str, prefix: str = ..., endpoint_url: str | None = ..., ...) -> S3Client:
    def put_object(self, key: str, body: bytes | BinaryIO, content_type: str = ...) -> dict:
    def upload_fileobj(self, key: str, fileobj: BinaryIO, part_size: int | None = ...) -> None:
    def head_object(self, key: str) -> dict | None:
    def list_objects(self, prefix: str = "") -> list[str]:
    def delete_object(self, key: str) -> None:
    def create_bucket(self) -> None:
    def build_key(self, relative_path: str) -> str:
```

Thin wrapper around boto3 S3 client with dependency injection. Accepts any `S3Ops`-compatible client via `__init__`. Use `from_env()` to create a client from environment variables. Supports MinIO, AWS S3, and any S3-compatible endpoint via `endpoint_url`. Uses multipart upload automatically for files larger than `part_size` (default 5MB).

**Raises:**
- `S3ClientError`: On S3 API errors or missing credentials.

---

### `push.core.state.PushState`

```python
@dataclass
class PushState:
    def __init__(self, state_path: Path) -> None:
    def is_pushed(self, local_path: str, checksum: str) -> bool:
    def record_push(self, local_path: str, s3_key: str, checksum: str) -> None:
    def save(self) -> None:
```

Tracks which files have been pushed to S3. Stores mapping of local paths to S3 keys and checksums. Persists to JSON file.

---

### `push.core.state.PushResult`

```python
@dataclass(frozen=True)
class PushResult:
    uploaded: int = 0
    skipped: int = 0
    failed: int = 0
    total: int = 0
    uploaded_files: list[str] = field(default_factory=list)
```

Immutable result of a push operation. `uploaded_files` contains the relative paths of files that were successfully uploaded to S3. This list is written to `.push_manifest.json` for cross-module coordination with `extract`.

---

### `push.core.state.UploadConfig`

```python
@dataclass(frozen=True)
class UploadConfig:
    include: set[str] | None = None
    exclude: set[str] | None = None
    overwrite: bool = False
    delete_after_push: bool = False
```

Frozen dataclass bundling upload parameters to satisfy the ≤3 arguments rule.
The `collect_files()` function automatically excludes `.push_state.json` from uploads.
`delete_after_push=True` deletes local files after successful S3 upload.

---

### `push.core.ops.S3Ops`

```python
class S3Ops(Protocol):
    def put_object(self, Bucket: str, Key: str, Body: bytes | BinaryIO, ContentType: str) -> dict: ...
    def upload_fileobj(self, Fileobj: BinaryIO, Bucket: str, Key: str, Config: object) -> None: ...
    def head_object(self, Bucket: str, Key: str) -> dict | None: ...
    def get_paginator(self, name: str) -> object: ...
    def delete_object(self, Bucket: str, Key: str) -> None: ...
    def create_bucket(self, Bucket: str) -> None: ...
    def head_bucket(self, Bucket: str) -> None: ...
```

Protocol defining S3 client operations. Enables dependency injection of any S3-compatible client (boto3, moto, test doubles) without depending on boto3 directly.

---

## Edge cases

### Interrupted uploads
- State is only updated after successful upload + checksum verification
- Partial uploads leave objects in S3 with incomplete data
- Re-running upload will re-upload (state not recorded)

### Checksum mismatch
- If a local file changes but state says it's already pushed, the file is skipped
- Use `config.overwrite=True` to force re-upload

### MinIO bucket creation
- `create_bucket()` handles race conditions: 409 Conflict is silently ignored
- For S3-compatible endpoints (with `endpoint_url`), `create_bucket()` is used
- For native AWS, `head_bucket()` is used instead

### Large files
- Files > 5MB use multipart upload automatically via boto3 TransferConfig
- `part_size` is configurable in S3Client constructor (default 5MB)

---

## Design decisions

| Decision | Rationale |
|---|---|
| S3Ops Protocol | Decouple from boto3, enable DI and testing |
| UploadConfig dataclass | Group ≤3 args, frozen for immutability |
| DI via __init__ | Infrastructure clients injected, never created internally |
| S3Client.from_env() | Classmethod for environment-based creation |
| build_key() public | No private method access from other modules |
| State separate (state.py) | Different responsibility — persistence vs upload logic |
| SHA-256 checksums | Same as extract module, consistent integrity verification |
| PushState tracking | Avoid re-uploading unchanged files |
| Continue on error | Uploading 300+ files — one failure shouldn't stop the run |
| Env vars for config | No config files needed for cloud environments |
| boto3 instead of minio SDK | boto3 supports any S3-compatible endpoint, more portable |
| No pydantic dependency | State management is simple enough for dataclasses |
| upload_fileobj → None | Matches boto3 real return type |
| .push_state.json auto-excluded | Prevents uploading internal state files |

---

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `S3_ENDPOINT_URL` | — | S3/MinIO endpoint (e.g. `http://localhost:9000`) |
| `S3_BUCKET` | — | S3 bucket name (required) |
| `S3_PREFIX` | `data` | Key prefix for uploaded objects |
| `AWS_ACCESS_KEY_ID` | — | S3 credentials |
| `AWS_SECRET_ACCESS_KEY` | — | S3 credentials |

---

## What is NOT in this module

- File downloading → see `extract/`
- Data transformation → see `transform/`
- Database loading → see `load/`

---

## Files to read if you need more context

| File | Why read it |
|---|---|
| `OVERVIEW.md` | Module flow and structure |
| `tests/test_core_upload.py` | Full unit test scenarios |
| `tests/test_e2e_client.py` | MinIO integration tests for S3Client |
| `tests/test_e2e_upload.py` | Full upload pipeline E2E tests |
