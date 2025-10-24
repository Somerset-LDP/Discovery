import logging
import json
from typing import Optional


class JsonFormatter(logging.Formatter):
    STANDARD_FIELDS = {
        'name', 'msg', 'args', 'created', 'filename', 'funcName', 'levelname',
        'levelno', 'lineno', 'module', 'msecs', 'message', 'pathname', 'process',
        'processName', 'relativeCreated', 'thread', 'threadName', 'exc_info',
        'exc_text', 'stack_info'
    }

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': self.formatTime(record),
            'level': record.levelname,
            'message': record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key not in self.STANDARD_FIELDS:
                log_data[key] = value

        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_data)


class CorrelationLogger:
    def __init__(self, base_logger: logging.Logger, correlation_id: Optional[str] = None):
        self.base_logger = base_logger
        self.correlation_id = correlation_id

    def _log(self, level: int, message: str, **kwargs):
        extra = kwargs.get('extra', {})
        if self.correlation_id:
            extra['correlationId'] = self.correlation_id
        kwargs['extra'] = extra
        self.base_logger.log(level, message, **kwargs)

    def debug(self, message: str, **kwargs):
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs):
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs):
        self._log(logging.ERROR, message, **kwargs)

