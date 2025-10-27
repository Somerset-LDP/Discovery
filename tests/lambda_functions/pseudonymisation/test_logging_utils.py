import logging
import json
import pytest
from lambda_functions.pseudonymisation.logging_utils import JsonFormatter, CorrelationLogger


@pytest.fixture
def logger():
    test_logger = logging.getLogger('test_logger')
    test_logger.setLevel(logging.DEBUG)
    test_logger.handlers = []
    return test_logger


@pytest.fixture
def json_formatter():
    return JsonFormatter()


@pytest.fixture
def basic_record():
    return logging.LogRecord(
        name='test',
        level=logging.INFO,
        pathname='test.py',
        lineno=10,
        msg='Test message',
        args=(),
        exc_info=None
    )


@pytest.fixture
def configured_logger(logger, json_formatter):
    handler = logging.StreamHandler()
    handler.setFormatter(json_formatter)
    logger.addHandler(handler)
    return logger, json_formatter


def get_log_data(formatter, record):
    return json.loads(formatter.format(record))


def test_json_formatter_basic_message(json_formatter, basic_record):
    result = json_formatter.format(basic_record)
    log_data = json.loads(result)

    assert log_data['level'] == 'INFO'
    assert log_data['message'] == 'Test message'
    assert 'timestamp' in log_data


def test_json_formatter_with_extra_fields(json_formatter, basic_record):
    basic_record.correlationId = 'test-123'
    basic_record.action = 'encrypt'
    basic_record.field_name = 'nhs_number'

    result = json_formatter.format(basic_record)
    log_data = json.loads(result)

    assert log_data['correlationId'] == 'test-123'
    assert log_data['action'] == 'encrypt'
    assert log_data['field_name'] == 'nhs_number'


def test_json_formatter_excludes_standard_fields(json_formatter, basic_record):
    result = json_formatter.format(basic_record)
    log_data = json.loads(result)

    assert 'name' not in log_data
    assert 'pathname' not in log_data
    assert 'lineno' not in log_data
    assert 'module' not in log_data


def test_correlation_logger_adds_correlation_id(configured_logger, caplog):
    logger, formatter = configured_logger

    correlation_id = 'test-correlation-123'
    correlation_logger = CorrelationLogger(logger, correlation_id)

    with caplog.at_level(logging.INFO):
        correlation_logger.info('Test message', extra={'action': 'encrypt'})

    assert len(caplog.records) == 1
    log_data = get_log_data(formatter, caplog.records[0])
    assert log_data['correlationId'] == correlation_id
    assert log_data['action'] == 'encrypt'


def test_correlation_logger_without_correlation_id(configured_logger, caplog):
    logger, formatter = configured_logger

    correlation_logger = CorrelationLogger(logger, None)

    with caplog.at_level(logging.INFO):
        correlation_logger.info('Test message', extra={'action': 'encrypt'})

    assert len(caplog.records) == 1
    log_data = get_log_data(formatter, caplog.records[0])
    assert 'correlationId' not in log_data
    assert log_data['action'] == 'encrypt'


def test_correlation_logger_merges_extra_fields(configured_logger, caplog):
    logger, formatter = configured_logger

    correlation_logger = CorrelationLogger(logger, 'corr-123')

    with caplog.at_level(logging.INFO):
        correlation_logger.info('Test', extra={'field_name': 'test_field'})

    assert len(caplog.records) == 1
    log_data = get_log_data(formatter, caplog.records[0])
    assert log_data['correlationId'] == 'corr-123'
    assert log_data['field_name'] == 'test_field'


def test_correlation_logger_all_log_levels(configured_logger, caplog):
    logger, formatter = configured_logger

    correlation_logger = CorrelationLogger(logger, 'test-id')

    with caplog.at_level(logging.DEBUG):
        correlation_logger.debug('Debug message')
        correlation_logger.info('Info message')
        correlation_logger.warning('Warning message')
        correlation_logger.error('Error message')

    assert len(caplog.records) == 4
    levels = [record.levelname for record in caplog.records]
    assert levels == ['DEBUG', 'INFO', 'WARNING', 'ERROR']

    for record in caplog.records:
        log_data = get_log_data(formatter, record)
        assert log_data['correlationId'] == 'test-id'
