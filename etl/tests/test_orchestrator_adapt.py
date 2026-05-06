"""Tests for Orchestrator._adapt_s3_to_manifest()."""

from __future__ import annotations

from etl.orchestrator import Orchestrator
from push.core.push_manifest import S3Object


class TestAdaptS3ToManifest:
    """Tests for _adapt_s3_to_manifest method."""

    def test_adapt_single_object(self) -> None:
        """Single S3Object -> manifest with rel_path."""
        orchestrator = Orchestrator()
        s3_objects = [S3Object(key="data/yellow/file.parquet")]
        result = orchestrator._adapt_s3_to_manifest(s3_objects, prefix="data")

        assert result == {
            "yellow/file.parquet": {
                "s3_key": "data/yellow/file.parquet",
            }
        }

    def test_adapt_multiple_objects(self) -> None:
        """Multiple S3Objects -> populated manifest."""
        orchestrator = Orchestrator()
        s3_objects = [
            S3Object(key="data/yellow/yellow_tripdata_2024-01.parquet"),
            S3Object(key="data/green/green_tripdata_2024-01.parquet"),
            S3Object(key="data/fhv/fhv_tripdata_2024-01.parquet"),
        ]
        result = orchestrator._adapt_s3_to_manifest(s3_objects, prefix="data")

        assert len(result) == 3
        assert "yellow/yellow_tripdata_2024-01.parquet" in result
        assert "green/green_tripdata_2024-01.parquet" in result
        assert "fhv/fhv_tripdata_2024-01.parquet" in result

    def test_adapt_respects_prefix(self) -> None:
        """Different prefix is respected."""
        orchestrator = Orchestrator()
        s3_objects = [
            S3Object(key="mydata/yellow/file.parquet"),
        ]
        result = orchestrator._adapt_s3_to_manifest(s3_objects, prefix="mydata")

        assert result == {
            "yellow/file.parquet": {
                "s3_key": "mydata/yellow/file.parquet",
            }
        }

    def test_adapt_empty_list(self) -> None:
        """Empty S3Object list -> empty manifest."""
        orchestrator = Orchestrator()
        result = orchestrator._adapt_s3_to_manifest([], prefix="data")
        assert result == {}

    def test_adapt_object_without_prefix_match(self) -> None:
        """Object not starting with prefix uses full key as rel_path."""
        orchestrator = Orchestrator()
        s3_objects = [S3Object(key="other/prefix/file.parquet")]
        result = orchestrator._adapt_s3_to_manifest(s3_objects, prefix="data")

        assert "other/prefix/file.parquet" in result
        assert result["other/prefix/file.parquet"]["s3_key"] == "other/prefix/file.parquet"
