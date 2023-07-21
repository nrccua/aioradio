"""Pytest logger."""

import logging

import pytest

from aioradio.logger import DatadogLogger

pytestmark = pytest.mark.asyncio


async def test_datadog_logger():
    """Check if the logger has json formatted messages."""

    logger = DatadogLogger(
        main_logger='pytest2',
        datadog_loggers=['pytest', 'pytest2'],
        log_level=logging.INFO,
    ).logger

    assert(logger.hasHandlers()) is True
    assert logger.info('Hello Pytest', extra={"special": "value", "run": 12}) is None

    try:
        5 / 'ten'
    except TypeError as err:
        assert logger.exception(err) is None

    for handler in logger.handlers:
        logger.removeHandler(handler)
