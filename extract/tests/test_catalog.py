"""Unit tests for the catalog module."""

from __future__ import annotations

from pathlib import Path

import pytest

from extract.core.catalog import Catalog
from extract.core.state import AVAILABLE_YEARS, CatalogEntry, DATA_TYPES, compute_sha256


class TestCatalogEntry:
    def test_url_format(self):
        entry = CatalogEntry("yellow", 2024, 1)
        assert entry.url == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    def test_url_format_with_single_digit_month(self):
        entry = CatalogEntry("green", 2023, 3)
        assert entry.url == "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-03.parquet"

    def test_filename(self):
        entry = CatalogEntry("fhv", 2022, 12)
        assert entry.filename == "fhv_tripdata_2022-12.parquet"

    def test_target_dir(self):
        entry = CatalogEntry("fhvhv", 2020, 6)
        assert entry.target_dir == "fhvhv"

    def test_frozen(self):
        entry = CatalogEntry("yellow", 2024, 1)
        with pytest.raises(Exception):
            entry.data_type = "green"

    @pytest.mark.parametrize(
        "data_type,year,month,expected_url",
        [
            ("yellow", 2009, 1, "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet"),
            ("green", 2025, 12, "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-12.parquet"),
            ("fhv", 2024, 7, "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-07.parquet"),
            ("fhvhv", 2019, 3, "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2019-03.parquet"),
        ],
    )
    def test_url_parametrized(self, data_type, year, month, expected_url):
        entry = CatalogEntry(data_type, year, month)
        assert entry.url == expected_url


class TestCatalog:
    def test_init_types_sorted(self):
        catalog = Catalog(types=["yellow", "green"])
        assert catalog.types == ["green", "yellow"]

    def test_init_types_single(self):
        catalog = Catalog(types=["fhv"])
        assert catalog.types == ["fhv"]

    def test_init_types_none(self):
        catalog = Catalog()
        assert catalog.types == DATA_TYPES

    def test_init_from_year_default(self):
        catalog = Catalog()
        assert catalog.from_year == min(AVAILABLE_YEARS)

    def test_init_to_year_default(self):
        catalog = Catalog()
        assert catalog.to_year == max(AVAILABLE_YEARS)

    def test_init_with_all_params(self):
        catalog = Catalog(types=["green"], from_year=2020, to_year=2022, max_entries=100)
        assert catalog.types == ["green"]
        assert catalog.from_year == 2020
        assert catalog.to_year == 2022
        assert catalog.max_entries == 100

    def test_defaults(self):
        catalog = Catalog()
        entries = catalog.generate()
        assert len(entries) > 0
        types_in_order = sorted(DATA_TYPES)
        for i in range(1, len(entries)):
            prev = entries[i - 1]
            curr = entries[i]
            if curr.data_type == prev.data_type:
                if curr.year == prev.year:
                    assert curr.month > prev.month
                else:
                    assert curr.year > prev.year

    def test_sorts_by_type_then_month(self):
        catalog = Catalog()
        entries = catalog.generate()
        for i in range(1, len(entries)):
            prev = entries[i - 1]
            curr = entries[i]
            if curr.data_type == prev.data_type:
                assert (curr.year, curr.month) > (prev.year, prev.month)
            else:
                assert curr.data_type > prev.data_type

    def test_filters_by_type(self):
        catalog = Catalog(types=["yellow"])
        entries = catalog.generate()
        assert all(e.data_type == "yellow" for e in entries)

    def test_filters_by_multiple_types(self):
        catalog = Catalog(types=["green", "yellow"])
        entries = catalog.generate()
        assert all(e.data_type in ("green", "yellow") for e in entries)
        types_seen = sorted(set(e.data_type for e in entries))
        assert types_seen == ["green", "yellow"]

    def test_filters_by_year_range(self):
        catalog = Catalog(from_year=2022, to_year=2023, types=["yellow"])
        entries = catalog.generate()
        assert all(2022 <= e.year <= 2023 for e in entries)
        assert all(e.data_type == "yellow" for e in entries)

    def test_year_range_ordering(self):
        """When from_year > to_year, range produces empty list."""
        catalog = Catalog(from_year=2024, to_year=2022, types=["yellow"])
        entries = catalog.generate()
        assert len(entries) == 0

    def test_count(self):
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
        assert catalog.count() == 12

    def test_len(self):
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
        assert len(catalog) == 12

    def test_all_types_present_when_no_filter(self):
        catalog = Catalog()
        entries = catalog.generate()
        types_present = set(e.data_type for e in entries)
        assert types_present == set(DATA_TYPES)

    def test_monthly_order_within_type(self):
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024)
        entries = catalog.generate()
        months = [e.month for e in entries]
        assert months == list(range(1, 13))

    def test_fhvhv_all_months_in_range(self):
        """fhvhv should generate all 12 months for a given year (no hardcoded filter)."""
        catalog = Catalog(types=["fhvhv"], from_year=2016, to_year=2016)
        entries = catalog.generate()
        assert len(entries) == 12

    def test_fhvhv_all_months_2026(self):
        """fhvhv should generate all 12 months for 2026 (no hardcoded filter)."""
        catalog = Catalog(types=["fhvhv"], from_year=2026, to_year=2026)
        entries = catalog.generate()
        assert len(entries) == 12

    def test_fhvhv_before_2016(self):
        """fhvhv before 2016 should also generate all months (dynamically discoverable)."""
        catalog = Catalog(types=["fhvhv"], from_year=2010, to_year=2010)
        entries = catalog.generate()
        assert len(entries) == 12

    def test_max_entries_limits_result(self, tmp_path: Path):
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024, max_entries=5)
        entries = catalog.generate()
        assert len(entries) == 5

    def test_max_entries_zero_returns_all(self):
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024, max_entries=0)
        entries = catalog.generate()
        assert len(entries) == 12

    def test_max_entries_exact(self):
        catalog = Catalog(types=["yellow"], from_year=2024, to_year=2024, max_entries=12)
        entries = catalog.generate()
        assert len(entries) == 12


class TestComputeSha256:
    def test_compute_sha256_known_content(self, tmp_path: Path):
        import hashlib
        test_file = tmp_path / "test.bin"
        test_file.write_bytes(b"hello world")
        expected = hashlib.sha256(b"hello world").hexdigest()
        assert compute_sha256(test_file) == expected

    def test_compute_sha256_empty_file(self, tmp_path: Path):
        import hashlib
        test_file = tmp_path / "empty.bin"
        test_file.write_bytes(b"")
        expected = hashlib.sha256(b"").hexdigest()
        assert compute_sha256(test_file) == expected

    def test_compute_sha256_large_file(self, tmp_path: Path):
        import hashlib
        test_file = tmp_path / "large.bin"
        content = b"x" * 100000
        test_file.write_bytes(content)
        expected = hashlib.sha256(content).hexdigest()
        assert compute_sha256(test_file) == expected
