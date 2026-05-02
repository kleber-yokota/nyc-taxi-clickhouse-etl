"""E2E tests for the downloader module using responses."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import responses

from extract.core.catalog import Catalog, CatalogEntry
from extract.core.downloader import run
from extract.core.state import compute_sha256
from extract.core.state_manager import State


class TestDownloadNewFiles:
    def test_download_new_files(self, download_dir: Path):
        """Test that new files are downloaded and state is persisted."""
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
            for entry in catalog.generate():
                rsps.add(
                    responses.GET,
                    entry.url,
                    body=b"fake" + entry.url.encode(),
                    status=200,
                    content_type="application/octet-stream",
                )
            result = run(data_dir=download_dir, types=["yellow"], from_year=2024, to_year=2024, mode="full")

        assert result["downloaded"] == 12
        assert result["failed"] == 0

        state = State(download_dir / ".download_state.json")
        assert len(state.checksums) == 12


class Test404Handled:
    def test_404_handled_gracefully(self, download_dir: Path):
        """Test that 404 responses are logged and recorded as known missing."""
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
            for entry in catalog.generate():
                rsps.add(responses.GET, entry.url, body="", status=404)

            result = run(data_dir=download_dir, types=["yellow"], from_year=2024, to_year=2024, mode="full")

        assert result["failed"] == 12
        assert result["downloaded"] == 0

        errors_log = download_dir / "errors" / "download_errors.log"
        assert errors_log.exists()
        lines = errors_log.read_text().strip().split("\n")
        assert len(lines) == 12

        missing_file = download_dir / "known_missing.txt"
        assert missing_file.exists()
        lines = missing_file.read_text().strip().split("\n")
        assert len(lines) == 12


class TestNetworkErrorHandled:
    def test_network_error_handled(self, download_dir: Path):
        """Test that network errors are logged and don't crash."""
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
            for entry in catalog.generate():
                rsps.add(responses.GET, entry.url, body=responses.ConnectionError("Network unreachable"))

            result = run(data_dir=download_dir, types=["yellow"], from_year=2024, to_year=2024, mode="full")

        assert result["failed"] == 12
        assert result["downloaded"] == 0

        errors_log = download_dir / "errors" / "download_errors.log"
        assert errors_log.exists()


class TestPartialDownload:
    def test_partial_download(self, download_dir: Path):
        """Test that download stops gracefully after failures."""
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            catalog = Catalog(types=["yellow", "green"], from_year=2024, to_year=2024)
            entries = catalog.generate()

            for i, entry in enumerate(entries):
                if i < 6:
                    rsps.add(
                        responses.GET,
                        entry.url,
                        body=b"fake" + entry.url.encode(),
                        status=200,
                        content_type="application/octet-stream",
                    )
                else:
                    rsps.add(responses.GET, entry.url, body="", status=404)

            result = run(
                data_dir=download_dir,
                types=["yellow", "green"],
                from_year=2024,
                to_year=2024,
                mode="full",
            )

        assert result["downloaded"] == 6
        assert result["failed"] == 18


class TestChecksumMismatch:
    def test_checksum_mismatch_backs_up_old_file(self, download_dir: Path):
        """Test that files with checksum mismatch get backed up before download."""
        entry = CatalogEntry("yellow", 2024, 1)

        target_dir = download_dir / entry.target_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        old_content = b"old content"
        (target_dir / entry.filename).write_bytes(old_content)

        state = State(download_dir / ".download_state.json")
        old_checksum = compute_sha256(target_dir / entry.filename)
        state.save(entry.url, old_checksum)

        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            rsps.add(
                responses.GET,
                entry.url,
                body=b"new content",
                status=200,
                content_type="application/octet-stream",
            )
            result = run(
                data_dir=download_dir,
                types=["yellow"],
                from_year=2024,
                to_year=2024,
                mode="full",
            )

        backup_path = target_dir / f"{entry.filename}.old"
        assert backup_path.exists()
        assert backup_path.read_bytes() == old_content


class TestStatePersisted:
    def test_state_is_persisted(self, download_dir: Path):
        """Test that state file is persisted after download."""
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
            for entry in catalog.generate():
                rsps.add(
                    responses.GET,
                    entry.url,
                    body=b"fake" + entry.url.encode(),
                    status=200,
                    content_type="application/octet-stream",
                )
            run(data_dir=download_dir, types=["yellow"], from_year=2024, to_year=2024, mode="full")

        state_file = download_dir / ".download_state.json"
        assert state_file.exists()

        data = json.loads(state_file.read_text())
        assert "checksums" in data
        assert len(data["checksums"]) == 12


class TestMultipleRunsSkipDownloaded:
    def test_multiple_runs_skip_downloaded(self, download_dir: Path):
        """Test that running download twice skips already downloaded files."""
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
            for entry in catalog.generate():
                rsps.add(
                    responses.GET,
                    entry.url,
                    body=b"fake",
                    status=200,
                    content_type="application/octet-stream",
                )
            result1 = run(
                data_dir=download_dir,
                types=["yellow"],
                from_year=2024,
                to_year=2024,
                mode="full",
            )

        assert result1["downloaded"] > 0

        # Second run should skip all files
        result2 = run(
            data_dir=download_dir,
            types=["yellow"],
            from_year=2024,
            to_year=2024,
            mode="incremental",
        )
        assert result2["skipped"] == 12
        assert result2["downloaded"] == 0


class TestHTTP500:
    def test_http_500_handled(self, download_dir: Path):
        """Test that 500 errors are logged correctly."""
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
            for entry in catalog.generate():
                rsps.add(responses.GET, entry.url, body="", status=500)

            result = run(
                data_dir=download_dir,
                types=["yellow"],
                from_year=2024,
                to_year=2024,
                mode="full",
            )

        assert result["failed"] == 12

        errors_log = download_dir / "errors" / "download_errors.log"
        assert errors_log.exists()
        lines = errors_log.read_text().strip().split("\n")
        entry = json.loads(lines[0])
        assert entry["type"] == "http_error"


class TestTmpCleaned:
    def test_tmp_file_removed_on_success(self, download_dir: Path):
        """Test that .download.tmp files are removed after successful download."""
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
            for entry in catalog.generate():
                rsps.add(
                    responses.GET,
                    entry.url,
                    body=b"fake",
                    status=200,
                    content_type="application/octet-stream",
                )
            run(data_dir=download_dir, types=["yellow"], from_year=2024, to_year=2024, mode="full")

        for root, dirs, files in download_dir.walk():
            for f in files:
                assert not str(f).endswith(".download.tmp")
