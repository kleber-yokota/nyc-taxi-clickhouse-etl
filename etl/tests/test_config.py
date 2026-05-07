"""Tests for etl.config."""

import datetime

from etl.config import ETLConfig


def test_default_values():
    config = ETLConfig()
    assert config.types is None
    assert config.from_year == 2009
    assert config.to_year == datetime.datetime.now().year
    assert config.mode == "incremental"
    assert config.delete_after_upload is False


def test_custom_values():
    config = ETLConfig(
        types={"yellow", "green"},
        from_year=2020,
        to_year=2023,
        mode="full",
        delete_after_upload=True,
    )
    assert config.types == {"yellow", "green"}
    assert config.from_year == 2020
    assert config.to_year == 2023
    assert config.mode == "full"
    assert config.delete_after_upload is True


def test_is_frozen():
    config = ETLConfig()
    try:
        config.mode = "full"
        assert False, "Expected AttributeError"
    except AttributeError:
        pass
