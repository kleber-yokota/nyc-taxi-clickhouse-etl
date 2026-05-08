"""ETL orchestrator — coordinates extract → upload pipeline."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .checkpoint import save_checkpoint
from .checkpoint_builder import CheckpointBuilder
from .checksum_impl import UploadChecksum
from .config import ETLConfig
from .manifest import Manifest
from upload.core.runner import get_existing_uploads
from .state import PipelineState

logger = logging.getLogger(__name__)


class Orchestrator:
    """Coordinates the full ETL pipeline: extract → upload."""

    def __init__(self, config: ETLConfig | None = None) -> None:
        self.config = config or ETLConfig()
        self._checksum_func = UploadChecksum().compute
        self._checkpoint_builder = CheckpointBuilder()

    def run(self) -> dict:
        load_dotenv()
        data_dir = self._init_data_dir()
        state = self._init_state()
        manifest = Manifest(data_dir)

        self._init_manifest(manifest, data_dir)
        return self._run_pipeline(data_dir, state, manifest)

    def _run_pipeline(
        self, data_dir: Path, state: PipelineState,
        manifest: Manifest,
    ) -> dict:
        """Execute pipeline with error handling."""
        try:
            self._run_extract(data_dir, state, manifest)
            self._run_upload(data_dir, state, manifest)
            state.complete()
            self._persist_checkpoint(data_dir, state)
            return state.result()
        except Exception as e:
            self._handle_failure(data_dir, state, e)
            raise

    def _init_manifest(self, manifest: Manifest, data_dir: Path) -> None:
        """Initialize manifest with existing uploads and apply mode."""
        existing = get_existing_uploads(data_dir)
        manifest.init(existing)
        manifest.apply_mode(self.config.mode)

    def _init_data_dir(self) -> Path:
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        return data_dir

    def _init_state(self) -> PipelineState:
        state = PipelineState()
        state.start()
        return state

    def _run_extract(self, data_dir: Path, state: PipelineState,
                     manifest: Manifest) -> None:
        logger.info("=== Extract starting (mode=%s) ===", self.config.mode)
        extract_start = time.monotonic()

        push_manifest = self._get_push_manifest(manifest)
        extract_result = self._extract(data_dir, manifest, push_manifest)

        self._record_extract_results(state, extract_start, extract_result)
        self._record_downloads(manifest, extract_result)

    def _get_push_manifest(self, manifest: Manifest) -> dict:
        """Load current push manifest for extract phase."""
        return manifest._load()

    def _record_extract_results(self, state: PipelineState,
                                extract_start: float,
                                extract_result: Any) -> None:
        """Record extract phase timing and counts into pipeline state."""
        extract_duration = time.monotonic() - extract_start
        state.mark_extract_done(
            downloaded=extract_result.downloaded,
            skipped=extract_result.skipped,
            failed=extract_result.failed,
            total=extract_result.total,
            duration=extract_duration,
        )

    def _record_downloads(self, manifest: Manifest,
                          extract_result: Any) -> None:
        """Mark each extracted file as downloaded in the manifest."""
        for entry in extract_result.entries:
            manifest.record_download(entry.rel_path, entry.checksum)

    def _extract(self, data_dir: Path, manifest: Manifest,
                 push_manifest: dict) -> Any:
        from extract.downloader.downloader import run as extract_run

        return extract_run(
            data_dir=str(data_dir),
            types=list(self.config.types) if self.config.types else None,
            from_year=self.config.from_year,
            to_year=self.config.to_year,
            mode=self.config.mode,
            push_manifest=push_manifest,
            checksum_func=self._checksum_func,
        )

    def _run_upload(self, data_dir: Path, state: PipelineState,
                    manifest: Manifest) -> None:
        logger.info("=== Upload starting ===")
        upload_start = time.monotonic()

        upload_result = self._upload(data_dir)

        self._record_upload_results(state, upload_start, upload_result)
        self._record_uploads(manifest, upload_result)

    def _record_upload_results(self, state: PipelineState,
                               upload_start: float,
                               upload_result: Any) -> None:
        """Record upload phase timing and counts into pipeline state."""
        upload_duration = time.monotonic() - upload_start
        state.mark_upload_done(
            uploaded=upload_result.uploaded,
            uploaded_files=[e.rel_path for e in upload_result.entries],
            duration=upload_duration,
        )

    def _record_uploads(self, manifest: Manifest,
                        upload_result: Any) -> None:
        """Mark each uploaded file in the manifest."""
        for entry in upload_result.entries:
            manifest.record_upload(entry.rel_path)

    def _upload(self, data_dir: Path) -> Any:
        from upload.core.runner import upload_from_env
        from upload.core.state import UploadConfig

        upload_config = UploadConfig(
            overwrite=self.config.mode == "full",
            delete_after_upload=self.config.delete_after_upload,
        )
        return upload_from_env(
            data_dir=str(data_dir),
            config=upload_config,
            checksum_func=self._checksum_func,
        )

    def _persist_checkpoint(self, data_dir: Path, state: PipelineState) -> None:
        checkpoint = self._checkpoint_builder.build_success(state)
        save_checkpoint(data_dir, checkpoint)

    def _handle_failure(self, data_dir: Path, state: PipelineState,
                        error: Exception) -> None:
        error_msg = _extract_error_message(error)
        state.fail(error_msg)
        checkpoint = self._checkpoint_builder.build_failure(state)
        save_checkpoint(data_dir, checkpoint)


def _extract_error_message(error: Exception) -> str:
    """Extract error message from exception chain."""
    if hasattr(error, "__cause__") and error.__cause__:
        return str(error.__cause__)
    return str(error)
