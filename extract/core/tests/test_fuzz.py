"""Fuzz tests for state, catalog, and error handling using atheris."""

import json
import os
import sys
import tempfile

import atheris

with atheris.instrument_imports(
    include=["extract.core.state", "extract.core.state_manager", "extract.core.catalog"]
):
    from extract.core.catalog import Catalog, CatalogEntry
    from extract.core.state import ErrorType, compute_sha256, build_url
    from extract.core.state_manager import State


@atheris.instrument_func
def TestOneInput(data):
    """Fuzz all extract module code paths with arbitrary byte sequences."""
    fdp = atheris.FuzzedDataProvider(data)

    try:
        # Branch 1: basic state operations
        url = fdp.ConsumeUnicodeNoSurrogates(200)
        error_type_raw = fdp.ConsumeIntInRange(0, 7)
        if error_type_raw < len(ErrorType):
            error_type = ErrorType(error_type_raw)
        else:
            error_type = ErrorType.UNKNOWN

        state = State()
        state.log_error(url, error_type, fdp.ConsumeUnicodeNoSurrogates(500))
        state.save(url, fdp.ConsumeUnicodeNoSurrogates(64))
        state.load()
        state.get_checksum(url)
        state.is_downloaded(url)

        # Branch 2: state reset
        if fdp.ConsumeBool():
            state.reset()
            assert state.checksums == {}
            assert not state.is_downloaded(url)

        # Branch 3: multiple saves
        n = fdp.ConsumeIntInRange(1, 100)
        for _ in range(n):
            u = fdp.ConsumeUnicodeNoSurrogates(100)
            state.save(u, fdp.ConsumeUnicodeNoSurrogates(64))

        state2 = State()
        state2.load()
        assert state.checksums == state2.checksums

        # Branch 4: build_url with various values
        type_index = fdp.ConsumeIntInRange(0, 3)
        data_types = ["yellow", "green", "fhv", "fhvhv"]
        data_type = data_types[type_index]
        year = fdp.ConsumeIntInRange(2009, 2030)
        month = fdp.ConsumeIntInRange(1, 12)
        url2 = build_url(data_type, year, month)
        assert data_type in url2
        assert f"{year}-{month:02d}.parquet" in url2

        # Branch 5: CatalogEntry creation
        entry = CatalogEntry(data_type, year, month)
        assert entry.data_type == data_type
        assert entry.url == url2
        assert entry.filename == f"{data_type}_tripdata_{year}-{month:02d}.parquet"
        assert entry.target_dir == data_type

        # Branch 6: catalog filtering
        from_year = fdp.ConsumeIntInRange(2009, 2024)
        to_year = fdp.ConsumeIntInRange(2009, 2030)
        catalog = Catalog(types=[data_type], from_year=from_year, to_year=to_year)
        entries = catalog.generate()
        assert len(entries) >= 0
        effective_from = min(from_year, to_year)
        effective_to = max(from_year, to_year)
        for e in entries:
            assert effective_from <= e.year <= effective_to
            assert e.data_type == data_type

        # Branch 7: checksum computation
        content = fdp.ConsumeBytes(100)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        checksum = compute_sha256(tmp_path)
        assert len(checksum) == 64

        # Branch 8: corrupt JSON state loading
        corrupt_content = fdp.ConsumeBytes(200)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as corrupt_tmp:
            corrupt_tmp.write(corrupt_content)
            corrupt_path = corrupt_tmp.name

        corrupt_state = State(state_path=corrupt_path)
        assert corrupt_state.checksums == {}

        # Cleanup
        os.unlink(tmp_path)
        os.unlink(corrupt_path)

    except (ValueError, OverflowError, UnicodeEncodeError, json.JSONDecodeError):
        return


atheris.Setup(sys.argv, TestOneInput)
atheris.Fuzz()
