"""Fuzz tests for state serialization and error handling using atheris."""

import sys

import atheris

with atheris.instrument_imports(include=["extract.core.state", "extract.core.state_manager"]):
    from extract.core.state import ErrorType
    from extract.core.state_manager import State


@atheris.instrument_func
def TestOneInput(data):
    """Fuzz state operations with arbitrary byte sequences."""
    fdp = atheris.FuzzedDataProvider(data)

    try:
        url = fdp.ConsumeUnicodeNoSurrogates(200)
        error_type_raw = fdp.ConsumeIntInRange(0, 5)
        error_type = ErrorType(error_type_raw) if error_type_raw < len(ErrorType) else ErrorType.UNKNOWN

        state = State()
        state.log_error(url, error_type, fdp.ConsumeUnicodeNoSurrogates(500))
        state.save(url, fdp.ConsumeUnicodeNoSurrogates(64))
        state.load()

    except (ValueError, OverflowError, UnicodeEncodeError):
        return


@atheris.instrument_func
def TestStateSerialization(data):
    """Fuzz state save/load roundtrip with various URL/checksum combinations."""
    fdp = atheris.FuzzedDataProvider(data)

    try:
        state = State()
        n = fdp.ConsumeIntInRange(1, 100)
        for _ in range(n):
            url = fdp.ConsumeUnicodeNoSurrogates(100)
            checksum = fdp.ConsumeUnicodeNoSurrogates(64)
            state.save(url, checksum)

        state2 = State()
        state2.load()
        assert state.checksums == state2.checksums

    except (ValueError, OverflowError):
        return


@atheris.instrument_func
def TestErrorTypeVariety(data):
    """Fuzz through all error types with various URLs and details."""
    fdp = atheris.FuzzedDataProvider(data)

    try:
        url = fdp.ConsumeUnicodeNoSurrogates(200)
        detail = fdp.ConsumeUnicodeNoSurrogates(500)

        for error_type in ErrorType:
            state = State()
            state.log_error(url, error_type, detail)

    except (ValueError, OverflowError, UnicodeEncodeError):
        return


atheris.Setup(sys.argv, TestOneInput)
atheris.Fuzz()
