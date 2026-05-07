"""ETL orchestrator — coordinates extract → upload pipeline."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .checkpoint import Checkpoint, save_checkpoint
from .checksum import Checksum
from .config import ETLConfig
from .manifest import add_entry, load_manifest, save_manifest
from .state import PipelineState

logger = logging.getLogger(__name__)


class Orchestrator:
    """Coordinates the full ETL pipeline: extract → upload."""

    def __init__(self, config: ETLConfig | None = None) -> None:
        """Initialize orchestrator.

        Args:
            config: ETL configuration. Uses defaults if None.
        """
        self.config = config or ETLConfig()
        self.checksum = Checksum()

    def run(self) -> dict:
        """Run the full ETL pipeline.

        Returns:
            Result dict with status and metrics.

        Raises:
            Exception: Re-raises on unrecoverable failure.
        """
        load_dotenv()
        data_dir = self._init_data_dir()
        state = self._init_state()

        try:
            return self._run_success_path(data_dir, state)
        except Exception as e:
            self._handle_failure(data_dir, state, e)
            raise

    def _init_data_dir(self) -> Path:
        """Initialize and return data directory."""
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        return data_dir

    def _init_state(self) -> PipelineState:
        """Create and start a new pipeline state."""
        state = PipelineState()
        state.start()
        return state

    def _run_success_path(self, data_dir: Path, state: PipelineState) -> dict:
        """Execute the successful pipeline path."""
        self._execute_extract(data_dir, state)
        self._execute_upload(data_dir, state)
        self._update_manifest(data_dir, state)
        state.complete()
        self._persist_checkpoint(data_dir, state)
        return state.result()

    def _execute_extract(self, data_dir: Path, state: PipelineState) -> None:
        """Run extract stage and update state."""
        logger.info("=== Extract starting (mode=%s) ===", self.config.mode)
        extract_start = time.monotonic()
        extract_result = self._execute_with_retry("extract", self._do_extract, data_dir)
        extract_duration = time.monotonic() - extract_start
        self._mark_extract_done(state, extract_result, extract_duration)

    def _execute_upload(self, data_dir: Path, state: PipelineState) -> None:
        """Run upload stage and update state."""
        logger.info("=== Upload starting ===")
        upload_start = time.monotonic()
        upload_result = self._execute_with_retry("upload", self._do_upload, data_dir)
        upload_duration = time.monotonic() - upload_start
        self._mark_upload_done(state, upload_result, upload_duration)

    def _do_extract(self, data_dir: Path) -> dict:
        """Execute extract operation."""
        from extract.downloader.downloader import run as extract_run

        return extract_run(
            data_dir=str(data_dir),
            types=list(self.config.types) if self.config.types else None,
            from_year=self.config.from_year,
            to_year=self.config.to_year,
            mode=self.config.mode,
            push_manifest=load_manifest(data_dir),
            checksum_func=self.checksum.compute_sha256,
        )

    def _do_upload(self, data_dir: Path):
        """Execute upload operation."""
        from upload.core.runner import upload_from_env
        from upload.core.state import UploadConfig

        upload_config = UploadConfig(
            overwrite=self.config.mode == "full",
            delete_after_upload=self.config.delete_after_upload,
        )
        return upload_from_env(
            data_dir=str(data_dir),
            config=upload_config,
            checksum_func=self.checksum.compute_sha256,
        )

    def _execute_with_retry(
        self,
        stage_name: str,
        operation: Callable[[Path], Any],
        data_dir: Path,
    ) -> Any:
        """Execute operation with exponential backoff retry.

        Args:
            stage_name: Name of the stage for logging.
            operation: Function to execute.
            data_dir: Path to the data directory.

        Returns:
            Result of the operation.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return operation(data_dir)
            except Exception as e:
                if attempt < max_retries - 1:
                    self._log_retry(stage_name, attempt, max_retries, e)
                    time.sleep(2 ** attempt)
                else:
                    logger.error("%s failed after %d attempts: %s", stage_name, max_retries, e)
                    raise
        raise RuntimeError(f"{stage_name} failed after retries")

    def _mark_extract_done(self, state: PipelineState, result: dict, duration: float) -> None:
        """Mark extract stage as done with metrics."""
        state.mark_extract_done(
            downloaded=result.get("downloaded", 0),
            skipped=result.get("skipped", 0),
            failed=result.get("failed", 0),
            total=result.get("total", 0),
            duration=duration,
        )

    def _mark_upload_done(self, state: PipelineState, result, duration: float) -> None:
        """Mark upload stage as done with metrics."""
        state.mark_upload_done(
            uploaded=result.uploaded,
            uploaded_files=result.uploaded_files,
            duration=duration,
        )

    def _update_manifest(self, data_dir: Path, state: PipelineState) -> None:
        """Update manifest with checksums for uploaded files."""
        metrics = state._metrics
        manifest = load_manifest(data_dir)
        for rel_path in metrics.upload.uploaded_files:
            self._add_manifest_entry(data_dir, manifest, rel_path)
        save_manifest(data_dir, manifest)

    def _add_manifest_entry(self, data_dir: Path, manifest: dict, rel_path: str) -> None:
        """Add a single entry to the manifest."""
        file_path = data_dir / rel_path
        checksum = self.checksum.compute_sha256(file_path)
        s3_key = f"data/{rel_path}"
        add_entry(manifest, rel_path, s3_key, checksum)

    def _persist_checkpoint(self, data_dir: Path, state: PipelineState) -> None:
        """Persist checkpoint to disk."""
        checkpoint = self._build_success_checkpoint(state)
        save_checkpoint(data_dir, checkpoint)

    def _build_success_checkpoint(self, state: PipelineState) -> Checkpoint:
        """Build checkpoint for successful completion."""
        metrics = state._metrics
        return Checkpoint(
            status=state.stage.value,
            extract=self._extract_metrics_dict(metrics.extract),
            upload=self._upload_metrics_dict(metrics.upload),
            total_duration_seconds=metrics.total_duration_seconds,
        )

    def _extract_metrics_dict(self, extract_metrics) -> dict:
        """Extract metrics as dict."""
        return {
            "duration_seconds": extract_metrics.duration_seconds,
            "downloaded": extract_metrics.downloaded,
            "skipped": extract_metrics.skipped,
            "failed": extract_metrics.failed,
            "total": extract_metrics.total,
        }

    def _upload_metrics_dict(self, upload_metrics) -> dict:
        """Upload metrics as dict."""
        return {
            "duration_seconds": upload_metrics.duration_seconds,
            "uploaded": upload_metrics.uploaded,
            "uploaded_files": upload_metrics.uploaded_files,
        }

    def _handle_failure(self, data_dir: Path, state: PipelineState, error: Exception) -> None:
        """Handle pipeline failure by saving checkpoint."""
        state.fail(str(error))
        checkpoint = self._build_failure_checkpoint(state)
        save_checkpoint(data_dir, checkpoint)

    def _build_failure_checkpoint(self, state: PipelineState) -> Checkpoint:
        """Build checkpoint for failed execution."""
        return Checkpoint(
            status=state.stage.value,
            error=state.error,
            total_duration_seconds=state._metrics.total_duration_seconds,
        )

    def _log_retry(self, stage: str, attempt: int, max_retries: int, error: Exception) -> None:
        """Log retry attempt with exponential backoff."""
        backoff = 2 ** attempt
        logger.warning(
            "%s failed (attempt %d/%d), retrying in %ds: %s",
            stage, attempt + 1, max_retries, backoff, error,
        )
