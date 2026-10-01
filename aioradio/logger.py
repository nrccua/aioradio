"""JSON-formatted console logging for applications."""

# pylint: disable=too-few-public-methods

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom Json Formatter."""

    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]):
        """Normalize default set of fields.

        Args:
            log_record (Dict[str, Any]): dict object containing log record info
            record (logging.LogRecord): contains all the info pertinent to the event being logged
            message_dict (Dict[str, Any]): message dict
        """

        super().add_fields(log_record, record, message_dict)
        if not log_record.get("timestamp"):
            now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            log_record["timestamp"] = now
        log_record["level"] = log_record["level"].upper() if log_record.get("level") else record.levelname

class JsonLogger:
    """Attach JSON stdout handlers to the named application loggers."""

    def __init__(
            self,
            main_logger='',
            logger_names: Optional[List[str]] = None,
            log_level=logging.INFO,
            log_format="%(timestamp)d %(level)d %(name)d %(message)d"
    ):

        self.logger = logging.getLogger(main_logger)
        self.logger.setLevel(log_level)
        self.log_level = log_level
        self.logger_names = set(logger_names or [])
        self.format = log_format
        self.add_handlers()

    def add_handlers(self):
        """Create log handlers."""

        for name in self.logger_names:
            logger = logging.getLogger(name)
            formatter = CustomJsonFormatter(self.format)
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(self.log_level)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False


class DatadogLogger(JsonLogger):
    """Compatibility wrapper for existing callers; no Datadog output is configured."""

    def __init__(
            self,
            main_logger='',
            datadog_loggers: Optional[List[str]] = None,
            log_level=logging.INFO,
            log_format="%(timestamp)d %(level)d %(name)d %(message)d"
    ):
        # Existing applications may still pass the tracing logger explicitly.
        logger_names = [name for name in datadog_loggers or [] if name != 'ddtrace']
        super().__init__(main_logger, logger_names, log_level, log_format)
