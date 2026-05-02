"""Catalog of available TLC parquet files, ordered by type then month."""

from __future__ import annotations

from .state import (
    AVAILABLE_MONTHS,
    AVAILABLE_YEARS,
    CatalogEntry,
    DATA_TYPES,
    FHVHV_MISSING_MONTHS,
    build_url,
)


class Catalog:
    """Generates ordered list of available TLC data files."""

    def __init__(
        self,
        types: list[str] | None = None,
        from_year: int | None = None,
        to_year: int | None = None,
    ) -> None:
        self.types = sorted(types) if types else DATA_TYPES
        self.from_year = from_year if from_year else min(AVAILABLE_YEARS)
        self.to_year = to_year if to_year else max(AVAILABLE_YEARS)

    def generate(self) -> list[CatalogEntry]:
        entries: list[CatalogEntry] = []
        for data_type in self.types:
            for year in range(self.from_year, self.to_year + 1):
                months = list(AVAILABLE_MONTHS)
                if data_type == "fhvhv" and year in FHVHV_MISSING_MONTHS:
                    missing = FHVHV_MISSING_MONTHS[year]
                    months = [m for m in months if m not in missing]
                for month in months:
                    entries.append(CatalogEntry(data_type, year, month))
        return entries

    def build_url(self, data_type: str, year: int, month: int) -> str:
        return build_url(data_type, year, month)

    def count(self) -> int:
        return len(self.generate())

    def __len__(self) -> int:
        return self.count()
