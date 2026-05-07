"""ETL orchestrator — coordinates extract → upload pipeline."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from dotenv import load_dotenv

from .checkpoint import save_checkpoint
from .checkpoint_builder import CheckpointBuilder
from .checksum_impl import ChecksumProvider, UploadChecksum
from .config import ETLConfig
from .manifest_updater import ManifestUpdater
from .retry import RetryPolicy
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
        self.checksum: ChecksumProvider = UploadChecksum()
        self._retry = RetryPolicy()
        self._checkpoint_builder = CheckpointBuilder()
        self._data_dir: Path | None = None
        self._state: PipelineState | None = None
        self._manifest_updater: ManifestUpdater | None = None

    def run(self) -> dict:
        """Run the full ETL pipeline.

        Returns:
            Result dict with status and metrics.

        Raises:
            Exception: Re-raises on unrecoverable failure.
        """
        load_dotenv()
        self._data_dir = self._init_data_dir()
        self._state = self._init_state()
        self._manifest_updater = ManifestUpdater(self._data_dir, self.checksum)

        try:
            return self._run_success_path()
        except Exception as e:
            self._handle_failure(e)
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

    def _run_success_path(self) -> dict:
        """Execute the successful pipeline path."""
        assert self._data_dir is not None
        assert self._state is not None
        self._execute_extract(self._state)
        self._execute_upload(self._state)
        self._update_manifest(self._state)
        self._state.complete()
        self._persist_checkpoint(self._state)
        return self._state.result()

    def _execute_extract(self, state: PipelineState) -> None:
        """Run extract stage and update state."""
        assert self._data_dir is not None
        logger.info("=== Extract starting (mode=%s) ===", self.config.mode)
        extract_start = time.monotonic()
        extract_result = self._retry.execute("extract", self._do_extract)
        extract_duration = time.monotonic() - extract_start
        self._mark_extract_done(state, extract_result, extract_duration)

    def _execute_upload(self, state: PipelineState) -> None:
        """Run upload stage and update state."""
        assert self._data_dir is not None
        logger.info("=== Upload starting ===")
        upload_start = time.monotonic()
        upload_result = self._retry.execute("upload", self._do_upload)
        upload_duration = time.monotonic() - upload_start
        self._mark_upload_done(state, upload_result, upload_duration)

    def _do_extract(self) -> dict:
        """Execute extract operation."""
        assert self._data_dir is not None
        from extract.downloader.downloader import run as extract_run

        return extract_run(
            data_dir=str(self._data_dir),
            types=list(self.config.types) if self.config.types else None,
            from_year=self.config.from_year,
            to_year=self.config.to_year,
            mode=self.config.mode,
            push_manifest=self._load_manifest(),
            checksum_func=self.checksum.compute,
        )

    def _do_upload(self) -> object:
        """Execute upload operation."""
        assert self._data_dir is not None
        from upload.core.runner import upload_from_env
        from upload.core.state import UploadConfig

        upload_config = UploadConfig(
            overwrite=self.config.mode == "full",
            delete_after_upload=self.config.delete_after_upload,
        )
        return upload_from_env(
            data_dir=str(self._data_dir),
            config=upload_config,
            checksum_func=self.checksum.compute,
        )

    def _load_manifest(self) -> dict:
        """Load current manifest from disk."""
        assert self._data_dir is not None
        from .manifest import load_manifest as _load

        return _load(self._data_dir)

    def _mark_extract_done(self, state: PipelineState, result: dict, duration: float) -> None:
        """Mark extract stage as done with metrics."""
        state.mark_extract_done(
            downloaded=result.get("downloaded", 0),
            skipped=result.get("skipped", 0),
            failed=result.get("failed", 0),
            total=result.get("total", 0),
            duration=duration,
        )

    def _mark_upload_done(self, state: PipelineState, result: object, duration: float) -> None:
        """Mark upload stage as done with metrics."""
        state.mark_upload_done(
            uploaded=result.uploaded,
            uploaded_files=result.uploaded_files,
            duration=duration,
        )

    def _update_manifest(self, state: PipelineState) -> None:
        """Update manifest with checksums for uploaded files."""
        assert self._data_dir is not None
        assert self._manifest_updater is not None
        metrics = state._metrics
        self._manifest_updater.update(metrics.upload.uploaded_files)

    def _persist_checkpoint(self, state: PipelineState) -> None:
        """Persist checkpoint to disk."""
        assert self._data_dir is not None
        checkpoint = self._checkpoint_builder.build_success(state)
        save_checkpoint(self._data_dir, checkpoint)

    def _handle_failure(self, error: Exception) -> None:
        """Handle pipeline failure by saving checkpoint."""
        assert self._data_dir is not None
        assert self._state is not None
        error_msg = self._extract_error_message(error)
        self._state.fail(error_msg)
        checkpoint = self._checkpoint_builder.build_failure(self._state)
        save_checkpoint(self._data_dir, checkpoint)

    def _extract_error_message(self, error: Exception) -> str:
        """Extract the root error message from a RetryError or other exception."""
        if hasattr(error, "__cause__") and error.__cause__ is not None:
            return str(error.__cause__)
        return str(error)
