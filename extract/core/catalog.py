"""Catalog of available TLC parquet files, ordered by type then month."""

from __future__ import annotations

from .state import (
    AVAILABLE_MONTHS,
    AVAILABLE_YEARS,
    CatalogEntry,
    DATA_TYPES,
    build_url,
)


class Catalog:
    """Generates ordered list of available TLC data files."""

    def __init__(
        self,
        types: list[str] | None = None,
        from_year: int | None = None,
        to_year: int | None = None,
        max_entries: int | None = None,
    ) -> None:
        self.types = sorted(types) if types else DATA_TYPES
        self.from_year = from_year if from_year else min(AVAILABLE_YEARS)
        self.to_year = to_year if to_year else max(AVAILABLE_YEARS)
        self.max_entries = max_entries

    def generate(self) -> list[CatalogEntry]:
        """Generate all catalog entries for the configured filters.

        Args:
            max_entries: Optional limit on number of entries returned.

        Returns:
            List of CatalogEntry objects, ordered by type then chronologically.
        """
        entries: list[CatalogEntry] = []
        for data_type in self.types:
            for year in range(self.from_year, self.to_year + 1):
                for month in AVAILABLE_MONTHS:
                    entries.append(CatalogEntry(data_type, year, month))
                    if self.max_entries and len(entries) >= self.max_entries:
                        return entries
        return entries

    def build_url(self, data_type: str, year: int, month: int) -> str:
        return build_url(data_type, year, month)

    def count(self) -> int:
        return len(self.generate())

    def __len__(self) -> int:
        return self.count()
