#!/usr/bin/env python3
"""
Unit tests for unit_converter.py convert_value_to_standard_unit function
"""
from typing import Any, Dict, Union
import pytest
from calculators.unit_converter import convert_value_to_standard_unit, UnitConversionError, UCUM_SYSTEM

# Test cases remain the same but now test the actual unit_converter function
    
# Test cases from raw_patients.json data
TEST_CASES = [
    # Patient 1 - LOINC codes with imperial units (needs conversion)
    {"patient": 1, "type": "29463-7", "system": "http://loinc.org", "value": 154, "unit": "lbs", "target_unit": "kg", "expected": 69.85},
    {"patient": 1, "type": "8302-2", "system": "http://loinc.org", "value": 65, "unit": "inches", "target_unit": "cm", "expected": 165.1},
    
    # Patient 2 - SNOMED codes with metric units (no conversion needed)
    {"patient": 2, "type": "27113001", "system": "http://snomed.info/sct", "value": 75, "unit": "kg", "target_unit": "kg", "expected": 75.0},
    {"patient": 2, "type": "50373000", "system": "http://snomed.info/sct", "value": 172, "unit": "cm", "target_unit": "cm", "expected": 172.0},
    
    # Patient 3 - SNOMED codes with metric units (no conversion needed)
    {"patient": 3, "type": "27113001", "system": "http://snomed.info/sct", "value": 35, "unit": "kg", "target_unit": "kg", "expected": 35.0},
    {"patient": 3, "type": "50373000", "system": "http://snomed.info/sct", "value": 140, "unit": "cm", "target_unit": "cm", "expected": 140.0},
    
    # Patient 4 - LOINC codes with imperial units (needs conversion)  
    {"patient": 4, "type": "29463-7", "system": "http://loinc.org", "value": 58.8, "unit": "lbs", "target_unit": "kg", "expected": 26.67},
    {"patient": 4, "type": "8302-2", "system": "http://loinc.org", "value": 47.2, "unit": "inches", "target_unit": "cm", "expected": 119.89},
]
@pytest.mark.parametrize("test_case", TEST_CASES)
def test_unit_conversion_cases(test_case: Dict[str, Any]) -> None:
    """Test individual unit conversion cases using convert_value_to_standard_unit function."""
    patient = test_case["patient"]
    obs_type = test_case["type"] 
    value = test_case["value"]
    input_unit = test_case["unit"]
    target_unit = test_case["target_unit"]
    expected = test_case["expected"]
    
    # Test the actual convert_value_to_standard_unit function
    converted_value = convert_value_to_standard_unit(value, input_unit, target_unit)
    
    # Assert the conversion result matches expected value (with tolerance for floating point)
    assert abs(converted_value - expected) < 0.1, (
        f"Patient {patient}, {obs_type}: Expected {expected}, got {converted_value:.2f} "
        f"for {value} {input_unit} -> {target_unit}"
    )


def test_no_conversion_needed() -> None:
    """Test that when input and target units are the same, no conversion occurs."""
    test_cases = [
        (75, "kg", "kg", 75.0),
        (172, "cm", "cm", 172.0),
        (35, "kg", "kg", 35.0),
    ]
    
    for value, input_unit, target_unit, expected in test_cases:
        converted_value = convert_value_to_standard_unit(value, input_unit, target_unit)
        assert converted_value == expected


def test_imperial_to_metric_conversions() -> None:
    """Test specific imperial to metric conversions used in BMI calculations."""
    # Test weight conversions (lbs to kg)
    weight_cases = [
        (154, "lbs", "kg", 69.85),  # Patient 1
        (58.8, "lbs", "kg", 26.67)  # Patient 4
    ]
    
    for value, input_unit, target_unit, expected in weight_cases:
        converted_value = convert_value_to_standard_unit(value, input_unit, target_unit)
        
        assert abs(converted_value - expected) < 0.1, (
            f"Weight conversion: {value} {input_unit} -> expected {expected} kg, got {converted_value:.2f} kg"
        )
    
    # Test height conversions (inches to cm)
    height_cases = [
        (65, "inches", "cm", 165.1),   # Patient 1  
        (47.2, "inches", "cm", 119.89) # Patient 4
    ]
    
    for value, input_unit, target_unit, expected in height_cases:
        converted_value = convert_value_to_standard_unit(value, input_unit, target_unit)
        
        assert abs(converted_value - expected) < 0.1, (
            f"Height conversion: {value} {input_unit} -> expected {expected} cm, got {converted_value:.2f} cm"
        )


def test_bmi_calculations_with_conversions() -> None:
    """Test that BMI calculations work correctly with unit conversions using convert_value_to_standard_unit."""
    # Test data: [patient_id, weight_value, weight_unit, height_value, height_unit, expected_bmi]
    patients = [
        (1, 154, "lbs", 65, "inches", 25.6),    # Imperial units
        (2, 75, "kg", 172, "cm", 25.4),         # Metric units
        (3, 35, "kg", 140, "cm", 17.9),         # Metric units
        (4, 58.8, "lbs", 47.2, "inches", 18.6) # Imperial units
    ]
    
    for patient_id, weight_val, weight_unit, height_val, height_unit, expected_bmi in patients:
        # Convert weight to kg using the unit converter
        weight_kg = convert_value_to_standard_unit(weight_val, weight_unit, "kg")
        
        # Convert height to meters using the unit converter
        if height_unit == "cm":
            height_m = convert_value_to_standard_unit(height_val, height_unit, "m")
        else:  # inches
            height_m = convert_value_to_standard_unit(height_val, height_unit, "m")
        
        # Calculate BMI
        bmi = weight_kg / (height_m ** 2)
        
        assert abs(bmi - expected_bmi) < 0.1, (
            f"Patient {patient_id}: Expected BMI {expected_bmi}, got {bmi:.1f} "
            f"(Weight: {weight_kg:.1f}kg, Height: {height_m:.2f}m)"
        )


def test_unsupported_unit_system() -> None:
    """Test that using an unsupported unit system raises ValueError."""
    with pytest.raises(ValueError, match="Unsupported standard unit system"):
        convert_value_to_standard_unit(100, "lbs", "kg", "http://example.com/units")


def test_invalid_input_value() -> None:
    """Test that non-numeric input values raise ValueError."""
    with pytest.raises(ValueError, match="Value must be numeric"):
        convert_value_to_standard_unit("not_a_number", "lbs", "kg")  # type: ignore


def test_unsupported_input_unit() -> None:
    """Test that unsupported input unit raises UnitConversionError."""
    with pytest.raises(UnitConversionError, match="Unsupported input unit"):
        convert_value_to_standard_unit(100, "unsupported_unit", "kg")


def test_unsupported_target_unit() -> None:
    """Test that unsupported target unit raises UnitConversionError."""
    with pytest.raises(UnitConversionError, match="Unsupported standard unit"):
        convert_value_to_standard_unit(100, "lbs", "unsupported_unit")


def test_ucum_bracket_notation() -> None:
    """Test UCUM bracket notation units like [in_i] and [lb_av]."""
    # Test UCUM inch notation
    result_inch = convert_value_to_standard_unit(12, "[in_i]", "cm")
    expected_inch = 30.48  # 12 inches = 30.48 cm
    assert abs(result_inch - expected_inch) < 0.1
    
    # Test UCUM pound notation  
    result_pound = convert_value_to_standard_unit(1, "[lb_av]", "kg")
    expected_pound = 0.453592  # 1 lb = 0.453592 kg
    assert abs(result_pound - expected_pound) < 0.001


def test_basic_unit_conversion_functionality() -> None:
    """Test basic unit conversion functionality with known values."""
    # Test simple conversions with exact expected results
    test_cases = [
        # Weight conversions
        (1, "kg", "g", 1000.0),
        (2.20462, "lbs", "kg", 1.0),
        
        # Length conversions  
        (100, "cm", "m", 1.0),
        (1, "m", "mm", 1000.0),
        (1, "in", "cm", 2.54),  # Use "in" instead of "inch" as supported by the module
    ]
    
    for value, input_unit, target_unit, expected in test_cases:
        result = convert_value_to_standard_unit(value, input_unit, target_unit)
        assert abs(result - expected) < 0.01, (
            f"Expected {expected}, got {result} for {value} {input_unit} -> {target_unit}"
        )


def test_zero_and_negative_values() -> None:
    """Test conversion with zero and negative values."""
    # Zero should convert to zero
    assert convert_value_to_standard_unit(0, "lbs", "kg") == 0.0
    
    # Negative values should work (though not typical for measurements)
    result = convert_value_to_standard_unit(-10, "lbs", "kg")
    expected = -10 * 0.453592  # Negative weight in kg
    assert abs(result - expected) < 0.001


def test_ucum_system_constant() -> None:
    """Test that the UCUM system constant is used correctly."""
    # This should work with default UCUM system
    result = convert_value_to_standard_unit(100, "lbs", "kg")
    assert result > 0
    
    # This should work when explicitly specifying UCUM system
    result2 = convert_value_to_standard_unit(100, "lbs", "kg", UCUM_SYSTEM)
    assert result == result2


def test_error_logging_and_messages() -> None:
    """Test that error messages are descriptive and helpful."""
    # Test unsupported input unit error message
    try:
        convert_value_to_standard_unit(100, "invalid_unit", "kg")
        assert False, "Should have raised UnitConversionError"
    except UnitConversionError as e:
        assert "Unsupported input unit" in str(e)
        assert "invalid_unit" in str(e)
        assert "Supported units:" in str(e)
    
    # Test unsupported target unit error message 
    try:
        convert_value_to_standard_unit(100, "lbs", "invalid_unit")
        assert False, "Should have raised UnitConversionError"
    except UnitConversionError as e:
        assert "Unsupported standard unit" in str(e)
        assert "invalid_unit" in str(e)