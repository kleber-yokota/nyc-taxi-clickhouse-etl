"""Tests for etl/orchestrator.py -- ETLConfig and Orchestrator basics."""

from __future__ import annotations

from pathlib import Path

import pytest
from etl.orchestrator import ETLConfig, Orchestrator, adapt_s3_to_manifest
from push.core.push_manifest import S3Object
from push.core.state import PushedEntry

from .conftest import FakeExtract, FakePush, FakeS3List


class TestETLConfigDefaults:
    """Tests for ETLConfig default values."""

    def test_defaults(self) -> None:
        """Default values are correct."""
        config = ETLConfig()
        assert config.data_dir == "data"
        assert config.bucket == ""
        assert config.prefix == "data"
        assert config.types == ["yellow", "green", "fhv", "fhvhv"]
        assert config.from_year == 2024
        assert config.to_year == 2024
        assert config.mode == "incremental"
        assert config.delete_after_push is True


class TestETLConfigCustom:
    """Tests for ETLConfig custom values."""

    def test_custom_values(self) -> None:
        """Custom values are preserved."""
        config = ETLConfig(
            data_dir="mydata",
            bucket="my-bucket",
            prefix="my-prefix",
            types=["yellow"],
            from_year=2023,
            to_year=2025,
            mode="full",
            delete_after_push=False,
        )
        assert config.data_dir == "mydata"
        assert config.bucket == "my-bucket"
        assert config.prefix == "my-prefix"
        assert config.types == ["yellow"]
        assert config.from_year == 2023
        assert config.to_year == 2025
        assert config.mode == "full"
        assert config.delete_after_push is False


class TestOrchestratorInit:
    """Tests for Orchestrator initialization."""

    def test_orchestrator_creates_data_dir(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract, fake_push: FakePush
    ) -> None:
        """Orchestrator creates data_dir if it doesn't exist."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        result = orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=fake_push,
        )

        assert tmp_data_dir.exists()
        assert "extract" in result
        assert "push" in result
        assert "reconciled" in result

    def test_orchestrator_with_no_config(self) -> None:
        """Orchestrator uses ETLConfig defaults when no config is provided."""
        orchestrator = Orchestrator()
        assert orchestrator.config.mode == "incremental"
        assert orchestrator.config.data_dir == "data"

    def test_run_calls_extract_with_correct_params(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract, fake_push: FakePush
    ) -> None:
        """Extract is called with correct parameters from config."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=5, skipped=0)
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=extract,
            push_client=fake_push,
        )

        assert extract.call_count == 1
        assert extract.last_data_dir == str(tmp_data_dir)
        assert extract.last_mode == "incremental"

    def test_run_calls_push_with_correct_params(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract, fake_push: FakePush
    ) -> None:
        """Push is called with correct parameters from config."""
        config = ETLConfig(
            data_dir=str(tmp_data_dir),
            bucket="test-bucket",
            prefix="test-prefix",
        )
        orchestrator = Orchestrator(config)

        push = FakePush(uploaded_count=0, uploaded_entries=[])
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=push,
        )

        assert push.call_count == 1
        assert push.last_bucket == "test-bucket"
        assert push.last_prefix == "test-prefix"


class TestRunIncrementalFlow:
    """Tests for incremental mode pipeline flow."""

    def test_incremental_flow_completes(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract, fake_push: FakePush
    ) -> None:
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

        result = orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=extract,
            push_client=push,
        )

        assert "extract" in result
        assert "push" in result
        assert "reconciled" in result

    def test_incremental_rebuilds_manifest_from_s3(
        self, tmp_data_dir: Path, fake_extract: FakeExtract, fake_push: FakePush
    ) -> None:
        """Incremental + no manifest -> rebuild via FakeS3List."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        s3_objects = [
            S3Object(key="data/yellow/file.parquet"),
            S3Object(key="data/green/file.parquet"),
        ]
        entries = [
            PushedEntry(
                rel_path="yellow/file.parquet",
                s3_key="data/yellow/file.parquet",
                checksum="abc",
            ),
            PushedEntry(
                rel_path="green/file.parquet",
                s3_key="data/green/file.parquet",
                checksum="def",
            ),
        ]
        s3_list = FakeS3List(objects=s3_objects)
        extract = FakeExtract(downloaded=2, skipped=0)
        push = FakePush(uploaded_count=2, uploaded_entries=entries)

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
        assert "green/file.parquet" in saved

    def test_incremental_uses_existing_manifest(
        self, tmp_data_dir: Path, fake_extract: FakeExtract, fake_push: FakePush
    ) -> None:
        """Incremental with existing manifest -> does not rebuild from S3."""
        # Pre-populate manifest
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text(
            '{"yellow/existing.parquet": {"s3_key": "data/yellow/existing.parquet", "checksum": "old"}}'
        )

        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        entries = [
            PushedEntry(
                rel_path="yellow/new.parquet",
                s3_key="data/yellow/new.parquet",
                checksum="new",
            ),
        ]
        extract = FakeExtract(downloaded=1, skipped=1)
        push = FakePush(uploaded_count=1, uploaded_entries=entries)

        orchestrator.run(
            s3_list_client=FakeS3List(),
            extract_client=extract,
            push_client=push,
        )

        import json
        saved = json.loads(manifest_path.read_text())
        assert "yellow/existing.parquet" in saved
        assert "yellow/new.parquet" in saved


class TestRunFullFlow:
    """Tests for full mode pipeline flow."""

    def test_full_flow_overwrites(
        self, tmp_data_dir: Path, fake_extract: FakeExtract, fake_push: FakePush
    ) -> None:
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

    def test_full_mode_skips_s3_rebuild(
        self, tmp_data_dir: Path, fake_extract: FakeExtract, fake_push: FakePush
    ) -> None:
        """Full mode should not rebuild manifest from S3."""
        config = ETLConfig(data_dir=str(tmp_data_dir), mode="full")
        orchestrator = Orchestrator(config)

        s3_list = FakeS3List(objects=[S3Object(key="data/yellow/file.parquet")])
        extract = FakeExtract(downloaded=10, skipped=0)
        push = FakePush(uploaded_count=10, uploaded_entries=[])

        result = orchestrator.run(
            s3_list_client=s3_list,
            extract_client=extract,
            push_client=push,
        )

        assert result["extract"]["downloaded"] == 10


class TestManifestSavedAfterPush:
    """Tests for manifest saving after push."""

    def test_manifest_saved_with_uploaded_entries(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract
    ) -> None:
        """Manifest is saved with uploaded_entries after push."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        entries = [
            PushedEntry(
                rel_path="yellow/file.parquet",
                s3_key="data/yellow/file.parquet",
                checksum="abc",
            ),
            PushedEntry(
                rel_path="green/file.parquet",
                s3_key="data/green/file.parquet",
                checksum="def",
            ),
        ]
        push = FakePush(uploaded_count=2, uploaded_entries=entries)

        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=push,
        )

        import json
        manifest_path = tmp_data_dir / ".push_manifest.json"
        assert manifest_path.exists()
        saved = json.loads(manifest_path.read_text())
        assert "yellow/file.parquet" in saved
        assert "green/file.parquet" in saved
        assert saved["yellow/file.parquet"]["checksum"] == "abc"
        assert saved["green/file.parquet"]["checksum"] == "def"

    def test_manifest_overwrites_existing_entry(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract
    ) -> None:
        """Updating an existing entry overwrites the old value."""
        manifest_path = tmp_data_dir / ".push_manifest.json"
        manifest_path.write_text(
            '{"yellow/file.parquet": {"s3_key": "data/old/path.parquet", "checksum": "old"}}'
        )

        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        entries = [
            PushedEntry(
                rel_path="yellow/file.parquet",
                s3_key="data/new/path.parquet",
                checksum="new",
            ),
        ]
        push = FakePush(uploaded_count=1, uploaded_entries=entries)

        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=push,
        )

        import json
        saved = json.loads(manifest_path.read_text())
        assert saved["yellow/file.parquet"]["checksum"] == "new"
        assert saved["yellow/file.parquet"]["s3_key"] == "data/new/path.parquet"


class TestResultStructure:
    """Tests for return value structure."""

    def test_result_contains_all_sections(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List
    ) -> None:
        """Return dict has extract, push, and reconciled sections."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=3, skipped=1)
        push = FakePush(uploaded_count=3, uploaded_entries=[])

        result = orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=extract,
            push_client=push,
        )

        assert "extract" in result
        assert "push" in result
        assert "reconciled" in result
        assert result["extract"]["downloaded"] == 3

    def test_reconcile_returns_details(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List
    ) -> None:
        """Reconciliation result has rebuilt and recovered keys."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=0, skipped=0)
        push = FakePush(uploaded_count=0, uploaded_entries=[])

        result = orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=extract,
            push_client=push,
        )

        assert "rebuilt" in result["reconciled"]
        assert "recovered" in result["reconciled"]


class TestBucketResolution:
    """Tests for bucket name resolution from environment."""

    def test_bucket_from_config_takes_precedence(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Config bucket is used over env var."""
        monkeypatch.setenv("S3_BUCKET", "env-bucket")
        config = ETLConfig(data_dir=str(tmp_data_dir), bucket="config-bucket")
        orchestrator = Orchestrator(config)

        push = FakePush(uploaded_count=0, uploaded_entries=[])
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=push,
        )

        assert push.last_bucket == "config-bucket"

    def test_bucket_from_env_when_config_empty(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Env var S3_BUCKET is used when config bucket is empty."""
        monkeypatch.setenv("S3_BUCKET", "env-bucket")
        config = ETLConfig(data_dir=str(tmp_data_dir), bucket="")
        orchestrator = Orchestrator(config)

        push = FakePush(uploaded_count=0, uploaded_entries=[])
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=push,
        )

        assert push.last_bucket == "env-bucket"

class TestOverwriteParamByMode:
    """Tests for overwrite parameter correctness by mode."""

    def test_incremental_mode_passes_overwrite_false(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract
    ) -> None:
        """Incremental mode passes overwrite=False to push."""
        config = ETLConfig(data_dir=str(tmp_data_dir), mode="incremental")
        orchestrator = Orchestrator(config)

        push = FakePush(uploaded_count=0, uploaded_entries=[])
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=push,
        )

        assert push.last_overwrite is False

    def test_full_mode_passes_overwrite_true(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract
    ) -> None:
        """Full mode passes overwrite=True to push."""
        config = ETLConfig(data_dir=str(tmp_data_dir), mode="full")
        orchestrator = Orchestrator(config)

        push = FakePush(uploaded_count=0, uploaded_entries=[])
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=push,
        )

        assert push.last_overwrite is True


class TestExtractCalledWithCorrectParams:
    """Tests that extract is called with correct parameters from orchestrator."""

    def test_extract_receives_types_from_config(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_push: FakePush
    ) -> None:
        """Extract receives the types list from config, not None."""
        config = ETLConfig(
            data_dir=str(tmp_data_dir),
            types=["yellow", "green"],
        )
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=1, skipped=0)
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=extract,
            push_client=fake_push,
        )

        assert extract.last_types == ["yellow", "green"]

    def test_extract_receives_push_manifest(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_push: FakePush
    ) -> None:
        """Extract receives the manifest dict, not None."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        extract = FakeExtract(downloaded=1, skipped=0)
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=extract,
            push_client=fake_push,
        )

        assert extract.last_push_manifest is not None
        assert isinstance(extract.last_push_manifest, dict)


class TestPushCalledWithCorrectParams:
    """Tests that push is called with correct parameters from orchestrator."""

    def test_push_receives_data_dir(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract
    ) -> None:
        """Push receives the correct data_dir string, not None."""
        config = ETLConfig(data_dir=str(tmp_data_dir))
        orchestrator = Orchestrator(config)

        push = FakePush(uploaded_count=0, uploaded_entries=[])
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=push,
        )

        assert push.last_data_dir == str(tmp_data_dir)

    def test_push_receives_overwrite_from_config(
        self, tmp_data_dir: Path, fake_s3_list: FakeS3List, fake_extract: FakeExtract
    ) -> None:
        """Push receives the overwrite boolean, not None."""
        config = ETLConfig(data_dir=str(tmp_data_dir), mode="full")
        orchestrator = Orchestrator(config)

        push = FakePush(uploaded_count=0, uploaded_entries=[])
        orchestrator.run(
            s3_list_client=fake_s3_list,
            extract_client=fake_extract,
            push_client=push,
        )

        assert push.last_overwrite is True


class TestAdaptS3ToManifest:
    """Tests for adapt_s3_to_manifest standalone function."""

    def test_adapt_strips_prefix_correctly(self) -> None:
        """S3 keys with prefix have prefix stripped for rel_path."""
        s3_objects = [
            S3Object(key="data/yellow/file.parquet"),
            S3Object(key="data/green/trip.parquet"),
        ]
        manifest = adapt_s3_to_manifest(s3_objects, "data")
        assert manifest == {
            "yellow/file.parquet": {"s3_key": "data/yellow/file.parquet"},
            "green/trip.parquet": {"s3_key": "data/green/trip.parquet"},
        }

    def test_adapt_keeps_key_without_prefix(self) -> None:
        """S3 keys without prefix are used as-is."""
        s3_objects = [S3Object(key="other/file.parquet")]
        manifest = adapt_s3_to_manifest(s3_objects, "data")
        assert manifest == {"other/file.parquet": {"s3_key": "other/file.parquet"}}





class TestRebuildManifest:
    """Tests for _rebuild_manifest method."""

    def test_rebuild_manifest_strips_prefix_correctly(self) -> None:
        """_rebuild_manifest strips the config prefix from S3 keys."""
        config = ETLConfig(prefix="data")
        orchestrator = Orchestrator(config)
        s3_list = FakeS3List(objects=[S3Object(key="data/yellow/file.parquet")])
        manifest = orchestrator._rebuild_manifest(s3_list)
        assert "yellow/file.parquet" in manifest

    def test_rebuild_manifest_uses_config_prefix(self) -> None:
        """_rebuild_manifest uses the prefix from config, not None."""
        config = ETLConfig(prefix="custom")
        orchestrator = Orchestrator(config)
        s3_list = FakeS3List(objects=[S3Object(key="custom/yellow/file.parquet")])
        manifest = orchestrator._rebuild_manifest(s3_list)
        assert "yellow/file.parquet" in manifest


class TestS3ListCalledWithCorrectBucket:
    """Tests that S3 list is called with correct bucket in incremental mode."""

    def test_s3_list_receives_bucket_from_config(
        self, tmp_data_dir: Path, fake_extract: FakeExtract, fake_push: FakePush
    ) -> None:
        """S3 list receives the bucket from config, not empty string."""
        config = ETLConfig(data_dir=str(tmp_data_dir), bucket="my-bucket")
        orchestrator = Orchestrator(config)

        s3_list = FakeS3List(objects=[])
        orchestrator.run(
            s3_list_client=s3_list,
            extract_client=fake_extract,
            push_client=fake_push,
        )

        assert s3_list.last_bucket == "my-bucket"

    def test_s3_list_receives_bucket_from_env_when_config_empty(
        self, tmp_data_dir: Path, fake_extract: FakeExtract, fake_push: FakePush,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """S3 list receives bucket from S3_BUCKET env when config is empty."""
        monkeypatch.setenv("S3_BUCKET", "env-bucket")
        config = ETLConfig(data_dir=str(tmp_data_dir), bucket="")
        orchestrator = Orchestrator(config)

        s3_list = FakeS3List(objects=[])
        orchestrator.run(
            s3_list_client=s3_list,
            extract_client=fake_extract,
            push_client=fake_push,
        )

        assert s3_list.last_bucket == "env-bucket"


import pytest
