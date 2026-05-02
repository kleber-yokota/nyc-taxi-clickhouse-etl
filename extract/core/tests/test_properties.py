"""Property-based tests for catalog and state using hypothesis."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st
from pathlib import Path

import pytest

from extract.core.catalog import Catalog, CatalogEntry
from extract.core.state import AVAILABLE_MONTHS, AVAILABLE_YEARS, DATA_TYPES, build_url


@given(
    data_type=st.sampled_from(["yellow", "green", "fhv", "fhvhv"]),
    year=st.integers(min_value=2009, max_value=2030),
    month=st.integers(min_value=1, max_value=12),
)
def test_url_format_is_valid(data_type: str, year: int, month: int):
    """URL always contains the correct type, year, and month."""
    url = build_url(data_type, year, month)
    assert url.startswith("https://d37ci6vzurychx.cloudfront.net/trip-data/")
    assert f"{data_type}_tripdata_{year}-{month:02d}.parquet" in url


@given(
    data_type=st.sampled_from(["yellow", "green", "fhv", "fhvhv"]),
    year=st.integers(min_value=2009, max_value=2030),
    month=st.integers(min_value=1, max_value=12),
)
def test_url_uniqueness(data_type: str, year: int, month: int):
    """Different (data_type, year, month) tuples produce different URLs."""
    url = build_url(data_type, year, month)
    assert url.endswith(".parquet")
    assert str(year) in url
    assert f"{month:02d}" in url


@given(
    data_type=st.sampled_from(["yellow", "green", "fhv", "fhvhv"]),
    year=st.integers(min_value=2009, max_value=2030),
    month=st.integers(min_value=1, max_value=12),
)
def test_entry_url_matches_build_url(data_type: str, year: int, month: int):
    """CatalogEntry.url matches the module-level build_url function."""
    entry = CatalogEntry(data_type, year, month)
    assert entry.url == build_url(data_type, year, month)


@given(
    data_type=st.sampled_from(["yellow", "green", "fhv", "fhvhv"]),
    year=st.integers(min_value=2009, max_value=2030),
    month=st.integers(min_value=1, max_value=12),
)
def test_entry_filename_matches_build_url(data_type: str, year: int, month: int):
    """CatalogEntry.filename matches the expected naming convention."""
    entry = CatalogEntry(data_type, year, month)
    assert entry.filename == f"{data_type}_tripdata_{year}-{month:02d}.parquet"


@given(
    data_type=st.sampled_from(["yellow", "green", "fhv", "fhvhv"]),
    year=st.integers(min_value=2009, max_value=2030),
    month=st.integers(min_value=1, max_value=12),
)
def test_entry_target_dir_matches_type(data_type: str, year: int, month: int):
    """CatalogEntry.target_dir always matches the data_type."""
    entry = CatalogEntry(data_type, year, month)
    assert entry.target_dir == data_type


@given(
    years=st.lists(st.integers(min_value=2020, max_value=2024), min_size=1, max_size=5, unique=True),
    types=st.lists(st.sampled_from(["yellow", "green"]), min_size=1, max_size=2, unique=True),
)
def test_catalog_sorting_is_monotonic(years: list[int], types: list[str]):
    """Entries within each type have monotonically increasing (year, month)."""
    years = sorted(years)
    catalog = Catalog(types=types, from_year=years[0], to_year=years[-1])
    entries = catalog.generate()

    for i in range(1, len(entries)):
        prev = entries[i - 1]
        curr = entries[i]
        if curr.data_type == prev.data_type:
            assert (curr.year, curr.month) > (prev.year, prev.month)


@given(
    types=st.lists(st.sampled_from(["yellow", "green", "fhv", "fhvhv"]), unique=True),
)
def test_type_filter_preserves_order(types: list[str]):
    """Filtered types appear in alphabetical order."""
    catalog = Catalog(types=types)
    entries = catalog.generate()
    if not entries:
        return

    types_in_entries = [e.data_type for e in entries]
    expected = sorted(set(types))
    for i in range(1, len(types_in_entries)):
        if types_in_entries[i] != types_in_entries[i - 1]:
            assert types_in_entries[i] > types_in_entries[i - 1]


@given(
    data_type=st.sampled_from(["yellow", "green", "fhv", "fhvhv"]),
    from_year=st.integers(min_value=2009, max_value=2024),
    to_year=st.integers(min_value=2010, max_value=2030),
)
def test_year_range_filters_correctly(
    data_type: str, from_year: int, to_year: int
):
    """All entries fall within the specified year range."""
    effective_from = min(from_year, to_year)
    effective_to = max(from_year, to_year)
    catalog = Catalog(types=[data_type], from_year=from_year, to_year=to_year)
    entries = catalog.generate()
    for e in entries:
        assert effective_from <= e.year <= effective_to


@given(
    data_type=st.sampled_from(["yellow", "green", "fhv", "fhvhv"]),
    from_year=st.integers(min_value=2020, max_value=2020),
    to_year=st.integers(min_value=2020, max_value=2020),
)
def test_single_year_produces_correct_count(
    data_type: str, from_year: int, to_year: int
):
    """Single year produces exactly 12 months of entries."""
    catalog = Catalog(types=[data_type], from_year=from_year, to_year=to_year)
    entries = catalog.generate()
    if data_type == "fhvhv" and from_year < 2016:
        assert len(entries) == 0
    else:
        assert len(entries) == 12
