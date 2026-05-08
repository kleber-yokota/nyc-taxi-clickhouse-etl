"""Upload orchestration — coordinates file collection, upload, and state tracking."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .checksum import compute_sha256
from .client import S3Client
from .filter import collect_files
from .state import UploadEntry, UploadResult, UploadState, UploadConfig

logger = logging.getLogger(__name__)

ChecksumFunc = Callable[[Path], str] | None


@dataclass(frozen=True)
class UploadOutcome:
    uploaded: int = 0
    skipped: int = 0


@dataclass
class UploadCounts:
    uploaded: int = 0
    skipped: int = 0
    failed: int = 0
    uploaded_rel_paths: set[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.uploaded_rel_paths is None:
            object.__setattr__(self, "uploaded_rel_paths", set())


def upload(
    data_dir: str | Path,
    client: S3Client,
    state: UploadState,
    config: UploadConfig | None = None,
    checksum_func: ChecksumFunc = None,
) -> UploadResult:
    """Upload files from data_dir to S3."""
    data_dir = Path(data_dir)
    result = _precheck(data_dir, config)
    if result is None:
        return UploadResult()

    files, resolved = result
    return _do_upload_all(data_dir, files, client, state, resolved, checksum_func)


def _precheck(
    data_dir: Path,
    config: UploadConfig | None,
) -> tuple[list[Path], UploadConfig] | None:
    """Return (files, config) or None if upload should be skipped."""
    if not _dir_exists(data_dir):
        _log_missing_dir(data_dir)
        return None
    resolved = _resolve_config(config)
    files = collect_files(data_dir, resolved.include, resolved.exclude)
    if not files:
        _log_no_files(data_dir)
        return None
    return files, resolved


def _log_missing_dir(data_dir: Path) -> None:
    """Log missing directory warning."""
    logger.warning("Data directory does not exist: %s", data_dir)


def _log_no_files(data_dir: Path) -> None:
    """Log no files found info."""
    logger.info("No files to upload in %s", data_dir)


def _resolve_config(config: UploadConfig | None) -> UploadConfig:
    """Return provided config or default."""
    return config if config is not None else UploadConfig()


def _dir_exists(data_dir: Path) -> bool:
    """Check if data directory exists."""
    return data_dir.exists()


def _do_upload_all(
    data_dir: Path,
    files: list[Path],
    client: S3Client,
    state: UploadState,
    config: UploadConfig,
    checksum_func: ChecksumFunc,
) -> UploadResult:
    """Execute upload phase and build result."""
    total = len(files)
    counts = _upload_files(files, data_dir, client, state, config, checksum_func)
    state.save()
    entries = _build_entries(state, counts.uploaded_rel_paths, data_dir)
    return _build_result(counts, total, entries)


def _build_result(
    counts: UploadCounts, total: int, entries: list[UploadEntry],
) -> UploadResult:
    """Build UploadResult from counts and entries."""
    return UploadResult(
        uploaded=counts.uploaded,
        skipped=counts.skipped,
        failed=counts.failed,
        total=total,
        entries=entries,
    )


def _upload_one(
    local_path: Path,
    data_dir: Path,
    client: S3Client,
    state: UploadState,
    config: UploadConfig,
    checksum_func: ChecksumFunc = None,
) -> UploadOutcome:
    """Upload a single file to S3."""
    rel_path = str(local_path.relative_to(data_dir))
    checksum = _compute_checksum(local_path, checksum_func)
    if _should_skip(local_path, checksum, state, config.overwrite):
        _log_skip(rel_path)
        return UploadOutcome(skipped=1)
    return _execute_upload(local_path, rel_path, client, state, config, checksum)


def _log_skip(rel_path: str) -> None:
    """Log skipped file debug message."""
    logger.debug("Skipping (already uploaded): %s", rel_path)


def _execute_upload(
    local_path: Path,
    rel_path: str,
    client: S3Client,
    state: UploadState,
    config: UploadConfig,
    checksum: str,
) -> UploadOutcome:
    """Execute the upload and post-upload steps."""
    s3_key = client.build_key(rel_path)
    _do_upload_and_record(local_path, s3_key, client, state, checksum)
    logger.info("Uploaded: %s -> s3://%s/%s", rel_path, client.bucket, s3_key)
    _post_upload(local_path, rel_path, config)
    return UploadOutcome(uploaded=1)


def _compute_checksum(local_path: Path,
                      checksum_func: ChecksumFunc) -> str:
    """Compute checksum using provided function or default."""
    if checksum_func:
        return checksum_func(local_path)
    return compute_sha256(local_path)


def _do_upload_and_record(
    local_path: Path, s3_key: str,
    client: S3Client, state: UploadState,
    checksum: str,
) -> None:
    """Upload file and record in state."""
    _do_upload(local_path, s3_key, client, checksum)
    state.record_upload(str(local_path), s3_key, checksum)


def _upload_files(
    files: list[Path],
    data_dir: Path,
    client: S3Client,
    state: UploadState,
    config: UploadConfig,
    checksum_func: ChecksumFunc,
) -> UploadCounts:
    """Upload all files and return aggregate counts."""
    counts = UploadCounts()
    for local_path in files:
        _process_upload(local_path, data_dir, client, state, config, checksum_func, counts)
    return counts


def _process_upload(
    local_path: Path,
    data_dir: Path,
    client: S3Client,
    state: UploadState,
    config: UploadConfig,
    checksum_func: ChecksumFunc,
    counts: UploadCounts,
) -> None:
    """Process a single file upload and update counts."""
    try:
        _do_single_upload(local_path, data_dir, client, state, config, checksum_func, counts)
    except Exception as e:
        logger.error("Unexpected error uploading %s: %s", local_path, e)
        counts.failed += 1


def _do_single_upload(
    local_path: Path,
    data_dir: Path,
    client: S3Client,
    state: UploadState,
    config: UploadConfig,
    checksum_func: ChecksumFunc,
    counts: UploadCounts,
) -> None:
    """Execute single upload and apply outcome."""
    result = _upload_one(local_path, data_dir, client, state, config, checksum_func)
    _apply_outcome(local_path, data_dir, counts, result)


def _apply_outcome(
    local_path: Path,
    data_dir: Path,
    counts: UploadCounts,
    result: UploadOutcome,
) -> None:
    """Apply upload outcome to counts."""
    counts.uploaded += result.uploaded
    counts.skipped += result.skipped
    if result.uploaded:
        rel_path = str(local_path.relative_to(data_dir))
        counts.uploaded_rel_paths.add(rel_path)


def _should_skip(local_path: Path, checksum: str, state: UploadState, overwrite: bool) -> bool:
    """Check if a file should be skipped."""
    return not overwrite and state.is_uploaded(str(local_path), checksum)


def _post_upload(local_path: Path, rel_path: str, config: UploadConfig) -> None:
    """Delete local file after upload if configured."""
    if config.delete_after_upload:
        local_path.unlink()
        logger.info("Deleted local file after upload: %s", rel_path)


def _build_entries(
    state: UploadState,
    uploaded_rel_paths: set[str],
    data_dir: Path,
) -> list[UploadEntry]:
    """Build UploadEntry list from state for uploaded paths."""
    entries: list[UploadEntry] = []
    for rel_path in uploaded_rel_paths:
        entry = _find_entry(state, rel_path, data_dir)
        if entry:
            entries.append(entry)
    return entries


def _find_entry(
    state: UploadState,
    rel_path: str,
    data_dir: Path,
) -> UploadEntry | None:
    """Find matching UploadEntry in state data."""
    for local_path, info in state._data.items():
        if str(Path(local_path).relative_to(data_dir)) == rel_path:
            return UploadEntry(
                rel_path=rel_path,
                s3_key=info["s3_key"],
                checksum=info["checksum"],
            )
    return None


def _do_upload(local_path: Path, s3_key: str, client: S3Client, checksum: str) -> None:
    """Perform the actual file upload to S3."""
    with open(local_path, "rb") as f:
        client.upload_fileobj(s3_key, f, checksum=checksum)


def recover_from_s3(data_dir: str | Path, client: S3Client) -> UploadState:
    """Populate UploadState by listing S3 objects."""
    data_dir = Path(data_dir)
    state = UploadState(data_dir / ".upload_state.json")

    for s3_key in client.list_objects(""):
        rel_path = s3_key.replace(f"{client.prefix}/", "", 1)
        head = client.head_object(s3_key)
        checksum = head.get("Metadata", {}).get("checksum", "") if head else ""
        state._data[str(data_dir / rel_path)] = {
            "s3_key": s3_key,
            "checksum": checksum,
        }

    return state
