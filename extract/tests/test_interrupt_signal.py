"""Unit tests for _handle_signal function."""

from __future__ import annotations

import logging

import pytest

from extract.core.interrupt import _handle_signal


class TestHandleSignal:
    def test_handle_signal_logs_with_signal_number(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.INFO, logger="extract.core.interrupt"):
            _handle_signal(2, None)

        assert any("Interrupt signal received (signal 2)" in record.message for record in caplog.records)

    def test_handle_signal_with_different_signal(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.INFO, logger="extract.core.interrupt"):
            _handle_signal(15, None)

        assert any("Interrupt signal received (signal 15)" in record.message for record in caplog.records)

    def test_handle_signal_with_frame(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.INFO, logger="extract.core.interrupt"):
            _handle_signal(2, object())

        assert any("Interrupt signal received (signal 2)" in record.message for record in caplog.records)
