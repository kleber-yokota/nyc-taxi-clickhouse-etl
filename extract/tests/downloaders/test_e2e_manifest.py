"""E2E tests for push manifest integration in download flow."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import responses

from extract.core.catalog import Catalog
from extract.core.push_manifest import PUSH_MANIFEST_FILE
from extract.downloader.downloader import run


class TestDownloadWithManifest:
    """E2E: download flow with push manifest integration."""

    @responses.activate
    def test_skips_files_in_manifest(self, download_dir: Path) -> None:
        """Download should skip files already in push manifest."""
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024, max_entries=3)
        entries = catalog.generate()

        # Mock all URLs as 200 OK
        for entry in entries:
            responses.add(
                responses.GET,
                entry.url,
                body=b"fake_parquet_data",
                status=200,
                content_type="application/octet-stream",
            )

        # Create manifest with first entry already in S3
        first_entry = entries[0]
        manifest = {
            f"{first_entry.target_dir}/{first_entry.filename}": {
                "s3_key": f"data/{first_entry.target_dir}/{first_entry.filename}",
                "checksum": "abc123",
            },
        }
        download_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = download_dir / PUSH_MANIFEST_FILE
        manifest_path.write_text(json.dumps(manifest))

        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        # First entry skipped (in manifest), others downloaded
        assert result["skipped"] >= 1
        assert result["downloaded"] + result["skipped"] == 3

    @responses.activate
    def test_downloads_all_when_no_manifest(self, download_dir: Path) -> None:
        """Download should fetch all files when no manifest exists."""
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024, max_entries=3)
        entries = catalog.generate()

        # Mock all URLs as 200 OK
        for entry in entries:
            responses.add(
                responses.GET,
                entry.url,
                body=b"fake_parquet_data",
                status=200,
                content_type="application/octet-stream",
            )

        download_dir.mkdir(parents=True, exist_ok=True)

        # No manifest file
        assert not (download_dir / PUSH_MANIFEST_FILE).exists()

        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        assert result["downloaded"] == 3
        assert result["skipped"] == 0

    @responses.activate
    def test_skips_all_when_manifest_has_all(self, download_dir: Path) -> None:
        """Download should skip all files when manifest contains everything."""
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024, max_entries=3)
        entries = catalog.generate()

        # Mock all URLs (won't be called but good to have)
        for entry in entries:
            responses.add(
                responses.GET,
                entry.url,
                body=b"fake_parquet_data",
                status=200,
                content_type="application/octet-stream",
            )

        download_dir.mkdir(parents=True, exist_ok=True)

        # Create manifest with ALL entries
        manifest = {}
        for entry in entries:
            rel_path = f"{entry.target_dir}/{entry.filename}"
            manifest[rel_path] = {
                "s3_key": f"data/{rel_path}",
                "checksum": "abc123",
            }
        manifest_path = download_dir / PUSH_MANIFEST_FILE
        manifest_path.write_text(json.dumps(manifest))

        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        # All skipped (in manifest), none downloaded
        assert result["skipped"] == 3
        assert result["downloaded"] == 0
        assert result["failed"] == 0

    @responses.activate
    def test_manifest_with_wrong_data_type(self, download_dir: Path) -> None:
        """Manifest with different data type should not skip current type."""
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024, max_entries=3)
        entries = catalog.generate()

        # Mock all URLs as 200 OK
        for entry in entries:
            responses.add(
                responses.GET,
                entry.url,
                body=b"fake_parquet_data",
                status=200,
                content_type="application/octet-stream",
            )

        download_dir.mkdir(parents=True, exist_ok=True)

        # Create manifest with GREEN data (not yellow)
        manifest = {
            "green/green_tripdata_2024-01.parquet": {
                "s3_key": "data/green/green_tripdata_2024-01.parquet",
                "checksum": "def456",
            },
        }
        manifest_path = download_dir / PUSH_MANIFEST_FILE
        manifest_path.write_text(json.dumps(manifest))

        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        # Yellow files should NOT be skipped (manifest has green)
        assert result["downloaded"] == 3
        assert result["skipped"] == 0

    @responses.activate
    def test_empty_manifest_does_not_skip(self, download_dir: Path) -> None:
        """Empty manifest file should not skip any files."""
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024, max_entries=3)
        entries = catalog.generate()

        # Mock all URLs as 200 OK
        for entry in entries:
            responses.add(
                responses.GET,
                entry.url,
                body=b"fake_parquet_data",
                status=200,
                content_type="application/octet-stream",
            )

        download_dir.mkdir(parents=True, exist_ok=True)

        # Create empty manifest
        manifest_path = download_dir / PUSH_MANIFEST_FILE
        manifest_path.write_text(json.dumps({}))

        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        assert result["downloaded"] == 3
        assert result["skipped"] == 0

    @responses.activate
    def test_manifest_with_invalid_json(self, download_dir: Path) -> None:
        """Invalid manifest file should be treated as empty."""
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024, max_entries=3)
        entries = catalog.generate()

        # Mock all URLs as 200 OK
        for entry in entries:
            responses.add(
                responses.GET,
                entry.url,
                body=b"fake_parquet_data",
                status=200,
                content_type="application/octet-stream",
            )

        download_dir.mkdir(parents=True, exist_ok=True)

        # Create invalid manifest
        manifest_path = download_dir / PUSH_MANIFEST_FILE
        manifest_path.write_text("not valid json")

        result = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="full",
            max_entries=3,
        )

        # Should download all (invalid manifest treated as empty)
        assert result["downloaded"] == 3
        assert result["failed"] == 0
