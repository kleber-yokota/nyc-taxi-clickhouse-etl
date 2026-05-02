"""Fuzz tests for state and known_missing using atheris."""

import sys
import tempfile

import atheris

with atheris.instrument_imports(
    include=["extract.core.state", "extract.core.state_manager", "extract.core.known_missing"]
):
    from extract.core.state import ErrorType, compute_sha256
    from extract.core.state_manager import State
    from extract.core.known_missing import KnownMissing


@atheris.instrument_func
def TestOneInput(data):
    """Fuzz state and known_missing operations."""
    fdp = atheris.FuzzedDataProvider(data)

    try:
        url = fdp.ConsumeUnicodeNoSurrogates(200)
        error_type_raw = fdp.ConsumeIntInRange(0, 5)
        error_type = ErrorType(error_type_raw) if error_type_raw < len(ErrorType) else ErrorType.UNKNOWN

        state = State()
        state.log_error(url, error_type, fdp.ConsumeUnicodeNoSurrogates(500))
        state.save(url, fdp.ConsumeUnicodeNoSurrogates(64))
        state.reset()
        state.is_downloaded(url)

        known_missing = KnownMissing()
        known_missing.add(url)
        assert known_missing.is_missing(url)

        # Roundtrip
        state2 = State()
        state2.load()
        assert state.checksums == state2.checksums

    except (ValueError, OverflowError, UnicodeEncodeError):
        return


atheris.Setup(sys.argv, TestOneInput)
atheris.Fuzz()
