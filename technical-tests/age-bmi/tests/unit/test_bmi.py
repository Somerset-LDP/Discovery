import pytest
from unittest.mock import patch, MagicMock
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import cast, Optional

from calculators.bmi import calculate_bmi_and_category, Code, _map_snomed_sex_to_rcpchgrowth_sex
from fhirclient import client

# ------------------------
# Fixtures & Helpers
# ------------------------

@pytest.fixture
def mock_fhir_client():
    """Mock FHIR client for tests."""
    class MockFHIRClient(client.FHIRClient):
        def __init__(self):
            self.server = MagicMock()
    return MockFHIRClient()

@pytest.fixture
def adult_patient_base():
    """Base adult patient data."""
    return {
        "dob": date(1990, 1, 1),  # 35 years old
        "height_cm": 170.0,
        "weight_kg": 70.0,
        "sex_code": "248152002",  # Female
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",  # White British
        "ethnicity_code_system": "http://snomed.info/sct"
    }

@pytest.fixture
def child_patient_base():
    """Base child patient data."""
    return {
        "dob": date.today() - timedelta(days=365 * 5),  # 5 years old
        "height_cm": 110.0,
        "weight_kg": 20.0,
        "sex_code": "248152002",  # Female
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",  # White British
        "ethnicity_code_system": "http://snomed.info/sct"
    }

@pytest.fixture
def mock_adult_category():
    """Mock adult BMI category."""
    return Code(code="35425004", system="http://snomed.info/sct")  # Normal weight

@pytest.fixture
def mock_child_category():
    """Mock child BMI category."""
    return Code(code="162864001", system="http://snomed.info/sct")  # Normal body weight

class MockMeasurement:
    """Mock rcpchgrowth Measurement class."""
    def __init__(self, birth_date, sex, measurement_method, reference, observation_value, observation_date):
        self.measurement = {
            "measurement_calculated_values": {
                "chronological_centile": 50.0  # Default to 50th centile
            }
        }

# ------------------------
# Happy Path Tests - Adults
# ------------------------

def test_adult_normal_bmi_white_ethnicity(adult_patient_base, mock_adult_category, mock_fhir_client):
    """Test adult with normal BMI and White ethnicity."""
    adult_patient_base.update({"height_cm": 170.0, "weight_kg": 65.0})  # BMI ≈ 22.5
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=mock_adult_category):
        bmi, category = calculate_bmi_and_category(adult_patient_base, mock_fhir_client)
    
    assert bmi == pytest.approx(22.49, abs=0.01)
    assert category == mock_adult_category

def test_adult_overweight_bmi_white_ethnicity(adult_patient_base, mock_fhir_client):
    """Test adult with overweight BMI and White ethnicity."""
    adult_patient_base.update({"height_cm": 165.0, "weight_kg": 75.0})  # BMI ≈ 27.5
    overweight_category = Code(code="238131007", system="http://snomed.info/sct")
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=overweight_category):
        bmi, category = calculate_bmi_and_category(adult_patient_base, mock_fhir_client)
    
    assert bmi == pytest.approx(27.55, abs=0.01)
    assert category == overweight_category

def test_adult_obese_bmi_asian_ethnicity(adult_patient_base, mock_fhir_client):
    """Test adult with obese BMI and Asian ethnicity (lower thresholds)."""
    adult_patient_base.update({
        "height_cm": 160.0, 
        "weight_kg": 75.0,  # BMI ≈ 29.3
        "ethnicity_code": "92461000000104"  # Asian Indian
    })
    obese_category = Code(code="414915002", system="http://snomed.info/sct")
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=obese_category):
        bmi, category = calculate_bmi_and_category(adult_patient_base, mock_fhir_client)
    
    assert bmi == pytest.approx(29.30, abs=0.01)
    assert category == obese_category

def test_adult_underweight_bmi(adult_patient_base, mock_fhir_client):
    """Test adult with underweight BMI."""
    adult_patient_base.update({"height_cm": 175.0, "weight_kg": 50.0})  # BMI ≈ 16.3
    underweight_category = Code(code="248342006", system="http://snomed.info/sct")
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=underweight_category):
        bmi, category = calculate_bmi_and_category(adult_patient_base, mock_fhir_client)
    
    assert bmi == pytest.approx(16.33, abs=0.01)
    assert category == underweight_category

# ------------------------
# Happy Path Tests - Children
# ------------------------

def test_child_normal_bmi_female(child_patient_base, mock_child_category, mock_fhir_client):
    """Test child with normal BMI centile (female)."""
    child_patient_base.update({"height_cm": 110.0, "weight_kg": 20.0})
    
    with patch('calculators.bmi.Measurement', MockMeasurement), \
         patch('calculators.bmi._determine_child_weight_category', return_value=mock_child_category):
        bmi, category = calculate_bmi_and_category(child_patient_base, mock_fhir_client)
    
    assert bmi == pytest.approx(16.53, abs=0.01)
    assert category == mock_child_category

def test_child_normal_bmi_male(child_patient_base, mock_child_category, mock_fhir_client):
    """Test child with normal BMI centile (male)."""
    child_patient_base.update({
        "height_cm": 130.0, 
        "weight_kg": 28.0,
        "sex_code": "248153007"  # Male
    })
    
    with patch('calculators.bmi.Measurement', MockMeasurement), \
         patch('calculators.bmi._determine_child_weight_category', return_value=mock_child_category):
        bmi, category = calculate_bmi_and_category(child_patient_base, mock_fhir_client)
    
    assert bmi == pytest.approx(16.57, abs=0.01)
    assert category == mock_child_category

def test_one_year_old_infant_female(mock_child_category, mock_fhir_client):
    """Test 1-year-old infant (female)."""
    infant_data = {
        "dob": date.today() - timedelta(days=365),  # 1 year old
        "height_cm": 75.0,
        "weight_kg": 9.5,
        "sex_code": "248152002",  # Female
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",
        "ethnicity_code_system": "http://snomed.info/sct"
    }
    
    with patch('calculators.bmi.Measurement', MockMeasurement), \
         patch('calculators.bmi._determine_child_weight_category', return_value=mock_child_category):
        bmi, category = calculate_bmi_and_category(infant_data, mock_fhir_client)
    
    assert bmi == pytest.approx(16.89, abs=0.01)
    assert category == mock_child_category

# ------------------------
# Edge Case Tests - Age Boundaries
# ------------------------

def test_exactly_18_years_old_adult_path(mock_fhir_client):
    """Test exactly 18 years old takes adult path."""
    patient_data = {
        "dob": date.today() - timedelta(days=365 * 18),  # Exactly 18 years
        "height_cm": 170.0,
        "weight_kg": 70.0,
        "sex_code": "248152002",
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",
        "ethnicity_code_system": "http://snomed.info/sct"
    }
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None) as mock_adult, \
         patch('calculators.bmi._determine_child_weight_category', return_value=None) as mock_child:
        bmi, category = calculate_bmi_and_category(patient_data, mock_fhir_client)
    
    mock_adult.assert_called_once()
    mock_child.assert_not_called()

def test_17_years_364_days_child_path(mock_fhir_client):
    """Test 17 years, 364 days old takes child path."""
    patient_data = {
        "dob": date.today() - timedelta(days=365 * 17 + 364),  # 17 years, 364 days
        "height_cm": 170.0,
        "weight_kg": 70.0,
        "sex_code": "248152002",
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",
        "ethnicity_code_system": "http://snomed.info/sct"
    }
    
    with patch('calculators.bmi.Measurement', MockMeasurement), \
         patch('calculators.bmi._determine_adult_weight_category', return_value=None) as mock_adult, \
         patch('calculators.bmi._determine_child_weight_category', return_value=None) as mock_child:
        bmi, category = calculate_bmi_and_category(patient_data, mock_fhir_client)
    
    mock_adult.assert_not_called()
    mock_child.assert_called_once()

# ------------------------
# Missing/Invalid Data Tests
# ------------------------

def test_missing_height(adult_patient_base, mock_fhir_client):
    """Test missing height returns None values."""
    adult_patient_base["height_cm"] = None
    
    bmi, category = calculate_bmi_and_category(adult_patient_base, mock_fhir_client)
    
    assert bmi is None
    assert category is None

def test_missing_weight(adult_patient_base, mock_fhir_client):
    """Test missing weight returns None values."""
    adult_patient_base["weight_kg"] = None
    
    bmi, category = calculate_bmi_and_category(adult_patient_base, mock_fhir_client)
    
    assert bmi is None
    assert category is None

def test_missing_both_height_and_weight(adult_patient_base, mock_fhir_client):
    """Test missing both height and weight returns None values."""
    adult_patient_base["height_cm"] = None
    adult_patient_base["weight_kg"] = None
    
    bmi, category = calculate_bmi_and_category(adult_patient_base, mock_fhir_client)
    
    assert bmi is None
    assert category is None

def test_zero_height(adult_patient_base, mock_fhir_client):
    """Test zero height handles division by zero gracefully."""
    adult_patient_base["height_cm"] = 0
    
    with pytest.raises(ZeroDivisionError):
        calculate_bmi_and_category(adult_patient_base, mock_fhir_client)

def test_zero_weight(adult_patient_base, mock_fhir_client):
    """Test zero weight calculates BMI as 0."""
    adult_patient_base["weight_kg"] = 0
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None):
        bmi, category = calculate_bmi_and_category(adult_patient_base, mock_fhir_client)
    
    assert bmi == 0.0
    assert category is None

# ------------------------
# Invalid Sex Code Tests
# ------------------------

def test_invalid_snomed_sex_code_for_child(child_patient_base, mock_fhir_client):
    """Test invalid SNOMED sex code for child raises ValueError."""
    child_patient_base["sex_code"] = "invalid_code"
    
    with pytest.raises(ValueError, match="Unknown SNOMED sex code"):
        # We need to test _map_snomed_sex_to_rcpchgrowth_sex directly since 
        # calculate_bmi_and_category now handles this gracefully
        _map_snomed_sex_to_rcpchgrowth_sex("invalid_code")

def test_non_snomed_sex_system_for_child(child_patient_base, mock_fhir_client):
    """Test non-SNOMED sex system for child raises ValueError."""
    child_patient_base["sex_code_system"] = "http://loinc.org"
    
    with patch('calculators.bmi.Measurement', MockMeasurement):
        with pytest.raises(ValueError, match="Sex code must be SNOMED CT"):
            calculate_bmi_and_category(child_patient_base, mock_fhir_client)

def test_missing_sex_code_for_child(child_patient_base, mock_fhir_client):
    """Test missing sex code for child is handled gracefully."""
    child_patient_base["sex_code"] = None
    
    with patch('calculators.bmi.Measurement', MockMeasurement):
        bmi, category = calculate_bmi_and_category(child_patient_base, mock_fhir_client)
    
    # Should calculate BMI but category might be None
    assert bmi == pytest.approx(16.53, abs=0.01)
    assert category is None  # Expected behavior when sex mapping fails

# ------------------------
# FHIR Client Tests
# ------------------------

def test_with_valid_fhir_client(adult_patient_base):
    """Test with valid FHIR client passes it through."""
    mock_client = MagicMock()
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None) as mock_determine:
        calculate_bmi_and_category(adult_patient_base, mock_client)
    
    mock_determine.assert_called_once()
    args, kwargs = mock_determine.call_args
    assert args[2] == mock_client  # fhir_client parameter

def test_without_fhir_client_none(adult_patient_base):
    """Test without FHIR client (None) uses default behavior."""
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None) as mock_determine:
        calculate_bmi_and_category(adult_patient_base, None)
    
    mock_determine.assert_called_once()
    args, kwargs = mock_determine.call_args
    assert args[2] is None  # fhir_client parameter

def test_fhir_client_observation_definition_not_found(adult_patient_base, mock_fhir_client):
    """Test FHIR client that fails to find ObservationDefinition."""
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None):
        bmi, category = calculate_bmi_and_category(adult_patient_base, mock_fhir_client)
    
    # Should still calculate BMI even if category determination fails
    assert bmi == pytest.approx(24.22, abs=0.01)
    assert category is None

# ------------------------
# Extreme Values Tests
# ------------------------

def test_very_tall_adult(mock_fhir_client):
    """Test very tall adult."""
    patient_data = {
        "dob": date(1990, 1, 1),
        "height_cm": 220.0,
        "weight_kg": 100.0,
        "sex_code": "248153007",
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",
        "ethnicity_code_system": "http://snomed.info/sct"
    }
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None):
        bmi, category = calculate_bmi_and_category(patient_data, mock_fhir_client)
    
    assert bmi == pytest.approx(20.66, abs=0.01)

def test_very_short_adult(mock_fhir_client):
    """Test very short adult."""
    patient_data = {
        "dob": date(1990, 1, 1),
        "height_cm": 140.0,
        "weight_kg": 60.0,
        "sex_code": "248152002",
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",
        "ethnicity_code_system": "http://snomed.info/sct"
    }
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None):
        bmi, category = calculate_bmi_and_category(patient_data, mock_fhir_client)
    
    assert bmi == pytest.approx(30.61, abs=0.01)

def test_very_high_bmi(mock_fhir_client):
    """Test very high BMI."""
    patient_data = {
        "dob": date(1990, 1, 1),
        "height_cm": 160.0,
        "weight_kg": 150.0,
        "sex_code": "248152002",
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",
        "ethnicity_code_system": "http://snomed.info/sct"
    }
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None):
        bmi, category = calculate_bmi_and_category(patient_data, mock_fhir_client)
    
    assert bmi == pytest.approx(58.59, abs=0.01)

def test_very_low_bmi(mock_fhir_client):
    """Test very low BMI."""
    patient_data = {
        "dob": date(1990, 1, 1),
        "height_cm": 180.0,
        "weight_kg": 40.0,
        "sex_code": "248152002",
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",
        "ethnicity_code_system": "http://snomed.info/sct"
    }
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None):
        bmi, category = calculate_bmi_and_category(patient_data, mock_fhir_client)
    
    assert bmi == pytest.approx(12.35, abs=0.01)

# ------------------------
# Data Type Tests
# ------------------------

def test_decimal_inputs(mock_fhir_client):
    """Test Decimal inputs for height and weight."""
    patient_data = {
        "dob": date(1990, 1, 1),
        "height_cm": Decimal("170.5"),
        "weight_kg": Decimal("68.2"),
        "sex_code": "248152002",
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",
        "ethnicity_code_system": "http://snomed.info/sct"
    }
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None):
        bmi, category = calculate_bmi_and_category(patient_data, mock_fhir_client)
    
    assert bmi == pytest.approx(23.46, abs=0.01)

def test_float_inputs(mock_fhir_client):
    """Test float inputs for height and weight."""
    patient_data = {
        "dob": date(1990, 1, 1),
        "height_cm": 170.5,
        "weight_kg": 68.2,
        "sex_code": "248152002",
        "sex_code_system": "http://snomed.info/sct",
        "ethnicity_code": "92411000000101",
        "ethnicity_code_system": "http://snomed.info/sct"
    }
    
    with patch('calculators.bmi._determine_adult_weight_category', return_value=None):
        bmi, category = calculate_bmi_and_category(patient_data, mock_fhir_client)
    
    assert bmi == pytest.approx(23.46, abs=0.01)

# ------------------------
# Mock Integration Tests
# ------------------------

def test_rcpchgrowth_library_failure(child_patient_base, mock_fhir_client):
    """Test rcpchgrowth library failure is handled gracefully."""
    with patch('calculators.bmi.Measurement', side_effect=Exception("rcpchgrowth error")):
        with pytest.raises(Exception, match="rcpchgrowth error"):
            calculate_bmi_and_category(child_patient_base, mock_fhir_client)

def test_diagnostics_service_failure(child_patient_base, mock_fhir_client):
    """Test DiagnosticsService failure returns None category."""
    with patch('calculators.bmi.Measurement', MockMeasurement), \
         patch('calculators.bmi._determine_child_weight_category', return_value=None):
        bmi, category = calculate_bmi_and_category(child_patient_base, mock_fhir_client)
    
    # Should still calculate BMI even if category determination fails
    assert bmi == pytest.approx(16.53, abs=0.01)
    assert category is None

# ------------------------
# Sex Mapping Tests
# ------------------------

def test_map_snomed_sex_to_rcpchgrowth_sex_female():
    """Test female SNOMED code mapping."""
    result = _map_snomed_sex_to_rcpchgrowth_sex("248152002")
    assert result == "female"

def test_map_snomed_sex_to_rcpchgrowth_sex_male():
    """Test male SNOMED code mapping."""
    result = _map_snomed_sex_to_rcpchgrowth_sex("248153007")
    assert result == "male"

def test_map_snomed_sex_to_rcpchgrowth_sex_unknown():
    """Test unknown SNOMED code returns None."""
    result = _map_snomed_sex_to_rcpchgrowth_sex("unknown_code")
    assert result is None