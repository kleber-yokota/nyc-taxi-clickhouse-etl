"""Checkpoint builder — constructs Checkpoint from PipelineState."""

from __future__ import annotations

from etl.checkpoint import Checkpoint
from etl.state import PipelineState


class CheckpointBuilder:
    """Builds Checkpoint snapshots from PipelineState.

    Separates checkpoint construction logic from the orchestrator.
    """

    def build_success(self, state: PipelineState) -> Checkpoint:
        """Build checkpoint for successful completion.

        Args:
            state: PipelineState with completed metrics.

        Returns:
            Checkpoint with all stage metrics.
        """
        metrics = state._metrics
        return Checkpoint(
            status=state.stage.value,
            extract=self._extract_metrics_dict(metrics.extract),
            upload=self._upload_metrics_dict(metrics.upload),
            total_duration_seconds=metrics.total_duration_seconds,
        )

    def build_failure(self, state: PipelineState) -> Checkpoint:
        """Build checkpoint for failed execution.

        Args:
            state: PipelineState with failure info.

        Returns:
            Checkpoint with status and error.
        """
        return Checkpoint(
            status=state.stage.value,
            error=state.error,
            total_duration_seconds=state._metrics.total_duration_seconds,
        )

    def _extract_metrics_dict(self, extract_metrics: object) -> dict:
        """Convert extract StageMetrics to dict."""
        return {
            "duration_seconds": extract_metrics.duration_seconds,
            "downloaded": extract_metrics.downloaded,
            "skipped": extract_metrics.skipped,
            "failed": extract_metrics.failed,
            "total": extract_metrics.total,
        }

    def _upload_metrics_dict(self, upload_metrics: object) -> dict:
        """Convert upload StageMetrics to dict."""
        return {
            "duration_seconds": upload_metrics.duration_seconds,
            "uploaded": upload_metrics.uploaded,
            "uploaded_files": upload_metrics.uploaded_files,
        }
