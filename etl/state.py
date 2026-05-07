"""Pipeline state management — tracks stage transitions and metrics."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class PipelineStage(str, Enum):
    NOT_RUNNING = "not_running"
    RUNNING = "running"
    EXTRACT_DONE = "extract_done"
    UPLOAD_DONE = "upload_done"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class StageMetrics:
    """Metrics for a single pipeline stage."""
    duration_seconds: float = 0.0
    downloaded: int = 0
    skipped: int = 0
    failed: int = 0
    total: int = 0
    uploaded: int = 0
    uploaded_files: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PipelineMetrics:
    """Aggregate metrics for the full pipeline."""
    total_duration_seconds: float = 0.0
    extract: StageMetrics = field(default_factory=StageMetrics)
    upload: StageMetrics = field(default_factory=StageMetrics)


class PipelineState:
    """Mutable state tracker for pipeline execution."""

    def __init__(self) -> None:
        self.stage = PipelineStage.NOT_RUNNING
        self.error: str | None = None
        self._started_at: float = 0.0
        self._metrics = PipelineMetrics()

    def start(self) -> None:
        """Transition to RUNNING."""
        self.stage = PipelineStage.RUNNING
        self._started_at = time.monotonic()

    def mark_extract_done(
        self,
        downloaded: int = 0,
        skipped: int = 0,
        failed: int = 0,
        total: int = 0,
        duration: float = 0.0,
    ) -> None:
        """Transition to EXTRACT_DONE with metrics."""
        self.stage = PipelineStage.EXTRACT_DONE
        self._metrics = PipelineMetrics(extract=self._build_extract_metrics(duration, downloaded, skipped, failed, total))

    def _build_extract_metrics(self, duration: float, downloaded: int, skipped: int, failed: int, total: int) -> StageMetrics:
        """Build extract stage metrics."""
        return StageMetrics(
            duration_seconds=duration,
            downloaded=downloaded,
            skipped=skipped,
            failed=failed,
            total=total,
        )

    def mark_upload_done(
        self,
        uploaded: int = 0,
        uploaded_files: list[str] | None = None,
        duration: float = 0.0,
    ) -> None:
        """Transition to UPLOAD_DONE with metrics."""
        self.stage = PipelineStage.UPLOAD_DONE
        self._metrics = PipelineMetrics(
            total_duration_seconds=time.monotonic() - self._started_at,
            extract=self._metrics.extract,
            upload=self._build_upload_metrics(duration, uploaded, uploaded_files),
        )

    def _build_upload_metrics(self, duration: float, uploaded: int, uploaded_files: list[str] | None) -> StageMetrics:
        """Build upload stage metrics."""
        return StageMetrics(
            duration_seconds=duration,
            uploaded=uploaded,
            uploaded_files=uploaded_files or [],
        )

    def complete(self) -> None:
        """Transition to COMPLETED."""
        self.stage = PipelineStage.COMPLETED
        self._metrics = PipelineMetrics(
            total_duration_seconds=time.monotonic() - self._started_at,
            extract=self._metrics.extract,
            upload=self._metrics.upload,
        )

    def fail(self, error: str) -> None:
        """Transition to FAILED with error message."""
        self.stage = PipelineStage.FAILED
        self.error = error
        self._metrics = PipelineMetrics(
            total_duration_seconds=time.monotonic() - self._started_at,
        )

    def result(self) -> dict:
        """Return final result dict."""
        return {
            "status": self.stage.value,
            "metrics": {
                "total_duration_seconds": self._metrics.total_duration_seconds,
                "extract": self._extract_metrics_section(),
                "upload": self._upload_metrics_section(),
            },
        }

    def _extract_metrics_section(self) -> dict:
        """Build extract metrics section."""
        m = self._metrics.extract
        return {
            "duration_seconds": m.duration_seconds,
            "downloaded": m.downloaded,
            "skipped": m.skipped,
            "failed": m.failed,
            "total": m.total,
        }

    def _upload_metrics_section(self) -> dict:
        """Build upload metrics section."""
        m = self._metrics.upload
        return {
            "duration_seconds": m.duration_seconds,
            "uploaded": m.uploaded,
            "uploaded_files": m.uploaded_files,
        }
