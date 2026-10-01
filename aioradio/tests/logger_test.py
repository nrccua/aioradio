"""Tests for JSON console logging."""

import json
import logging

from aioradio.logger import DatadogLogger, JsonLogger


def test_json_logger_emits_structured_console_logs(capsys):
    """Application metadata stays structured without Datadog-specific tags."""
    configured = JsonLogger(main_logger='aioradio-test', logger_names=['aioradio-test'])
    logger = configured.logger

    try:
        logger.info('Hello Pytest', extra={'special': 'value', 'run': 12})
        log_entry = json.loads(capsys.readouterr().out)
        assert log_entry['level'] == 'INFO'
        assert log_entry['message'] == 'Hello Pytest'
        assert log_entry['special'] == 'value'
        assert log_entry['run'] == 12
        assert 'timestamp' in log_entry
        assert 'ddtags' not in log_entry
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)


def test_legacy_logger_ignores_tracing_channel(capsys):
    """Old callers keep application console logs but not tracing logs."""
    trace_logger = logging.getLogger('ddtrace')
    existing_handlers = list(trace_logger.handlers)
    configured = DatadogLogger(
        main_logger='aioradio-legacy-test',
        datadog_loggers=['aioradio-legacy-test', 'ddtrace'],
    )

    try:
        assert configured.logger_names == {'aioradio-legacy-test'}
        assert trace_logger.handlers == existing_handlers
        configured.logger.info('Legacy caller')
        assert json.loads(capsys.readouterr().out)['message'] == 'Legacy caller'
    finally:
        for handler in list(configured.logger.handlers):
            configured.logger.removeHandler(handler)


def test_legacy_logger_has_no_implicit_handlers():
    """No tracing or Datadog handler is installed by default."""
    configured = DatadogLogger(main_logger='aioradio-default-test')

    assert configured.logger_names == set()
    assert configured.logger.handlers == []
