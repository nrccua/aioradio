"""Generic logger logging to console or using json_log_formatter when logging
in docker for cleaner datadog logging."""

# pylint: disable=too-few-public-methods

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List

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

        ddtags = self.get_ddtags(record, reserved=self._skip_fields)
        if ddtags:
            log_record['ddtags'] = ddtags
        super().add_fields(log_record, record, message_dict)
        if not log_record.get("timestamp"):
            now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            log_record["timestamp"] = now
        log_record["level"] = log_record["level"].upper() if log_record.get("level") else record.levelname

    @staticmethod
    def get_ddtags(record: logging.LogRecord, reserved: Dict[str, Any]) -> str:
        """Add datadog tags in the format datadog expects.

        Args:
            record (logging.LogRecord): contains all the info pertinent to the event being logged
            reserved (dict[str, Any]): reserved logging keys

        Returns:
            str: concatenated string of k-v pairs
        """

        tags = {k: v for k, v in record.__dict__.items() if k not in reserved}
        return ','.join([f"{k}:{v}" for k, v in tags.items()])

class DatadogLogger():
    """Custom class for JSON Formatter to include level and name."""

    def __init__(
            self,
            main_logger='',
            datadog_loggers=List[str],
            log_level=logging.INFO,
            log_format="%(timestamp)d %(level)d %(name)d %(message)d"
    ):

        self.logger = logging.getLogger(main_logger)
        self.logger.setLevel(log_level)
        self.log_level = log_level
        self.datadog_loggers = set(datadog_loggers + ['ddtrace']) if datadog_loggers else ['ddtrace']
        self.format = log_format
        self.add_handlers()

    def add_handlers(self):
        """Create log handlers."""

        for name in self.datadog_loggers:
            logger = logging.getLogger(name)
            formatter = CustomJsonFormatter(self.format)
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(self.log_level)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False
