"""Pytest logger."""

import json
import logging

import pytest

from aioradio.logger import DatadogLogger

pytestmark = pytest.mark.asyncio


async def test_datadog_logger(capsys):
    """Check if the logger has json formatted messages."""

    configured_logger = DatadogLogger(
        main_logger='pytest2',
        datadog_loggers=['pytest', 'pytest2'],
        log_level=logging.INFO,
    )
    logger = configured_logger.logger

    assert configured_logger.datadog_loggers == {'pytest', 'pytest2'}
    assert(logger.hasHandlers()) is True
    assert logger.info('Hello Pytest', extra={"special": "value", "run": 12}) is None
    log_entry = json.loads(capsys.readouterr().out.splitlines()[0])
    assert log_entry['level'] == 'INFO'
    assert log_entry['message'] == 'Hello Pytest'

    try:
        5 / 'ten'
    except TypeError as err:
        assert logger.exception(err) is None

    for handler in logger.handlers:
        logger.removeHandler(handler)


async def test_datadog_logger_has_no_implicit_loggers():
    """Only explicitly configured loggers receive a handler."""
    configured_logger = DatadogLogger(main_logger='pytest-default')

    assert configured_logger.datadog_loggers == set()
    assert configured_logger.logger.handlers == []
