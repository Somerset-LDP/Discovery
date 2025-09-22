import pytest
from pipeline import validate_record, calculate_bmi

# =========================
# Mock records for testing
# =========================
valid_record = {
    'patient_id': '1',
    'dob': '1980-05-12',
    'height': '180',
    'height_unit': 'cm',
    'weight': '75',
    'weight_unit': 'kg',
    'observation_time': '2025-09-22T10:00:00Z'
}

missing_height_record = {
    'patient_id': '2',
    'dob': '1990-08-30',
    'height': '',
    'height_unit': '',
    'weight': '75',
    'weight_unit': 'kg',
    'observation_time': '2025-09-22T10:00:00Z'
}

invalid_unit_record = {
    'patient_id': '3',
    'dob': '1975-02-20',
    'height': '65',
    'height_unit': 'inches',
    'weight': '150',
    'weight_unit': 'lb',
    'observation_time': '2025-09-22T10:00:00Z'
}

missing_observation_time_record = {
    'patient_id': '4',
    'dob': '2000-11-10',
    'height': '170',
    'height_unit': 'cm',
    'weight': '70',
    'weight_unit': 'kg',
    'observation_time': ''
}

# =========================
# Tests for validation
# =========================
def test_validate_record_valid():
    is_valid, errors = validate_record(valid_record)
    assert is_valid
    assert errors == []

def test_validate_record_missing_height():
    is_valid, errors = validate_record(missing_height_record)
    assert not is_valid
    assert "Missing height/unit" in errors

def test_validate_record_invalid_unit():
    is_valid, errors = validate_record(invalid_unit_record)
    assert not is_valid
    assert "Unknown height unit: inches" in errors
    assert "Unknown weight unit: lb" in errors

def test_validate_record_missing_observation_time():
    is_valid, errors = validate_record(missing_observation_time_record)
    assert not is_valid
    assert "Missing observation_time" in errors

# =========================
# Tests for BMI calculation
# =========================
def test_calculate_bmi_valid():
    bmi = calculate_bmi(float(valid_record['height']), float(valid_record['weight']))
    expected_bmi = round(75 / (180/100)**2, 2)
    assert bmi == expected_bmi

def test_calculate_bmi_missing_height():
    bmi = calculate_bmi(None, float(valid_record['weight']))
    assert bmi is None

def test_calculate_bmi_missing_weight():
    bmi = calculate_bmi(float(valid_record['height']), None)
    assert bmi is None
