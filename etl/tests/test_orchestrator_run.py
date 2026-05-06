"""Tests for Orchestrator.run() -- full pipeline flow with fakes."""

from __future__ import annotations

from pathlib import Path

from etl.orchestrator import ETLConfig, Orchestrator
from push.core.push_manifest import S3Object
from push.core.state import PushedEntry, PushResult

from .conftest import FakeExtract, FakePush, FakeS3List


def _make_push_result(
    uploaded_entries: list[PushedEntry] | None = None,
) -> PushResult:
    """Create a mock PushResult for testing.

    Args:
        uploaded_entries: List of PushedEntry records.

    Returns:
        PushResult instance.
    """
    return PushResult(
        uploaded=2,
        skipped=0,
        failed=0,
        total=2,
        uploaded_files=["yellow/file.parquet", "green/file.parquet"],
        uploaded_entries=uploaded_entries or [],
    )


class TestRunIncrementalFlow:
    """Tests for incremental mode pipeline flow."""

    def test_incremental_flow(self, tmp_data_dir: Path) -> None:
        """Incremental flow: extract -> push -> manifest update."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        entries = [
            PushedEntry(
                rel_path="yellow/yellow_tripdata_2024-01.parquet",
                s3_key="data/yellow/yellow_tripdata_2024-01.parquet",
                checksum="abc123",
            ),
        ]
        extract = FakeExtract(downloaded=5, skipped=0)
        push = FakePush(uploaded_count=2, uploaded_entries=entries)
        s3_list = FakeS3List()

        result = orchestrator.run(
            s3_list_client=s3_list,
            extract_client=extract,
            push_client=push,
        )

        assert "extract" in result
        assert "push" in result
        assert "reconciled" in result


class TestRunFullFlow:
    """Tests for full mode pipeline flow."""

    def test_full_flow(self, tmp_data_dir: Path) -> None:
        """Full flow: extract (reset) -> push (overwrite)."""
        config = ETLConfig(data_dir=str(tmp_data_dir), mode="full")
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=10, skipped=0)
        push = FakePush(uploaded_count=10, uploaded_entries=[])

        result = orchestrator.run(
            extract_client=extract,
            push_client=push,
        )

        assert "extract" in result
        assert "push" in result
        assert result["extract"]["downloaded"] == 10


class TestManifestPassedToExtract:
    """Tests for manifest passing to extract."""

    def test_manifest_passed_to_extract(self, tmp_data_dir: Path) -> None:
        """Extract receives existing manifest."""
        existing_manifest = {
            "yellow/yellow_tripdata_2024-01.parquet": {
                "s3_key": "data/yellow/yellow_tripdata_2024-01.parquet",
                "checksum": "existing",
            },
        }
        manifest_path = tmp_data_dir / ".push_manifest.json"
        import json
        manifest_path.write_text(json.dumps(existing_manifest))

        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=0, skipped=0)
        push = FakePush(uploaded_count=0, uploaded_entries=[])

        orchestrator.run(
            s3_list_client=FakeS3List(),
            extract_client=extract,
            push_client=push,
        )

        assert extract.call_count == 1
        assert extract.last_data_dir == str(tmp_data_dir)


class TestPushReceivesBucketAndPrefix:
    """Tests for push receiving correct bucket and prefix."""

    def test_push_receives_bucket_and_prefix(self, tmp_data_dir: Path) -> None:
        """Push receives bucket and prefix from config."""
        config = ETLConfig(
            data_dir=str(tmp_data_dir),
            bucket="custom-bucket",
            prefix="custom-prefix",
        )
        orchestrator = Orchestrator(config)

        push = FakePush(uploaded_count=0, uploaded_entries=[])
        s3_list = FakeS3List()

        orchestrator.run(
            s3_list_client=s3_list,
            extract_client=FakeExtract(),
            push_client=push,
        )

        assert push.last_bucket == "custom-bucket"
        assert push.last_prefix == "custom-prefix"


class TestManifestSavedAfterPush:
    """Tests for manifest saving after push."""

    def test_manifest_saved_after_push(self, tmp_data_dir: Path) -> None:
        """Manifest is saved with uploaded_entries after push."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        entries = [
            PushedEntry(
                rel_path="yellow/file.parquet",
                s3_key="data/yellow/file.parquet",
                checksum="abc",
            ),
        ]
        push = FakePush(uploaded_count=1, uploaded_entries=entries)
        s3_list = FakeS3List()

        orchestrator.run(
            s3_list_client=s3_list,
            extract_client=FakeExtract(),
            push_client=push,
        )

        manifest_path = tmp_data_dir / ".push_manifest.json"
        assert manifest_path.exists()

        import json
        saved = json.loads(manifest_path.read_text())
        assert "yellow/file.parquet" in saved
        assert saved["yellow/file.parquet"]["checksum"] == "abc"


class TestResultContainsExtractAndPush:
    """Tests for return value structure."""

    def test_result_contains_extract_and_push(self, tmp_data_dir: Path) -> None:
        """Return dict has both extract and push results."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=3, skipped=1)
        push = FakePush(uploaded_count=3, uploaded_entries=[])
        s3_list = FakeS3List()

        result = orchestrator.run(
            s3_list_client=s3_list,
            extract_client=extract,
            push_client=push,
        )

        assert "extract" in result
        assert "push" in result
        assert "reconciled" in result
        assert result["extract"]["downloaded"] == 3


class TestReconcileRebuildsMissing:
    """Tests for reconciliation logic."""

    def test_reconcile_rebuilds_missing(self, tmp_data_dir: Path) -> None:
        """Divergence: missing file triggers rebuild."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=0, skipped=0)
        push = FakePush(uploaded_count=0, uploaded_entries=[])
        s3_list = FakeS3List()

        result = orchestrator.run(
            s3_list_client=s3_list,
            extract_client=extract,
            push_client=push,
        )

        assert "rebuilt" in result["reconciled"]
        assert "recovered" in result["reconciled"]


class TestRebuildManifestFromS3:
    """Tests for manifest rebuild from S3 in incremental mode."""

    def test_rebuild_manifest_from_s3(self, tmp_data_dir: Path) -> None:
        """Incremental + no manifest -> rebuild via FakeS3List."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        s3_objects = [
            S3Object(key="data/yellow/file.parquet"),
        ]
        entries = [
            PushedEntry(
                rel_path="yellow/file.parquet",
                s3_key="data/yellow/file.parquet",
                checksum="abc",
            ),
        ]
        s3_list = FakeS3List(objects=s3_objects)
        extract = FakeExtract(downloaded=0, skipped=0)
        push = FakePush(uploaded_count=1, uploaded_entries=entries)

        orchestrator.run(
            s3_list_client=s3_list,
            extract_client=extract,
            push_client=push,
        )

        manifest_path = tmp_data_dir / ".push_manifest.json"
        assert manifest_path.exists()

        import json
        saved = json.loads(manifest_path.read_text())
        assert "yellow/file.parquet" in saved

    def test_incremental_uses_cached_manifest(self, tmp_data_dir: Path) -> None:
        """Incremental with existing manifest -> uses cached, not S3."""
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text(
            '{"yellow/cached.parquet": {"s3_key": "data/yellow/cached.parquet"}}'
        )

        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=0, skipped=1)
        push = FakePush(uploaded_count=0, uploaded_entries=[])

        # No s3_list_client provided - should NOT call S3 because manifest exists
        orchestrator.run(
            extract_client=extract,
            push_client=push,
        )

        import json
        saved = json.loads(manifest_path.read_text())
        assert "yellow/cached.parquet" in saved

    def test_full_mode_never_rebuilds_manifest(self, tmp_data_dir: Path) -> None:
        """Full mode never rebuilds manifest from S3."""
        config = ETLConfig(data_dir=str(tmp_data_dir), mode="full")
        orchestrator = Orchestrator(config)

        s3_list = FakeS3List(objects=[S3Object(key="data/yellow/file.parquet")])
        extract = FakeExtract(downloaded=5, skipped=0)
        push = FakePush(uploaded_count=5, uploaded_entries=[])

        orchestrator.run(
            s3_list_client=s3_list,
            extract_client=extract,
            push_client=push,
        )

        manifest_path = tmp_data_dir / ".push_manifest.json"
        assert manifest_path.exists()
