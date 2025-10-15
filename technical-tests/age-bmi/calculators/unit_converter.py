"""
Unit converter for healthcare observations using UCUM standard.

This module provides utilities for converting between units of measurement
commonly used in healthcare data, particularly for height, weight, and BMI calculations.
Uses the Pint library for robust unit conversion with UCUM compliance.
"""

import logging
from typing import Union
from pint import UnitRegistry

# Get a logger for this module
logger = logging.getLogger(__name__)

# UCUM system identifier
UCUM_SYSTEM = "http://unitsofmeasure.org"

# Map common healthcare units to Pint-compatible names
UNIT_MAPPING = {
    # Length units
    'inches': 'inch',
    'in': 'inch', 
    'cm': 'centimeter',
    'm': 'meter',
    'mm': 'millimeter',
    
    # Mass/Weight units  
    'lbs': 'pound',
    'lb': 'pound',
    'kg': 'kilogram',
    'g': 'gram',
    
    # Handle UCUM bracket notation
    '[in_i]': 'inch',
    '[lb_av]': 'pound'
}

class UnitConversionError(Exception):
    """Raised when unit conversion fails"""
    pass


def convert_value_to_standard_unit(
    value: Union[int, float], 
    input_unit: str, 
    standard_unit: str, 
    standard_unit_system: str = UCUM_SYSTEM
) -> float:
    """
    Convert value from input_unit to standard_unit using Pint for UCUM-compliant conversions.
    
    Args:
        value: Numeric value to convert
        input_unit: Source unit as string (e.g., 'lbs', 'inches', 'm')
        standard_unit: Target unit as string (e.g., 'kg', 'cm')
        standard_unit_system: Must be http://unitsofmeasure.org
        
    Returns:
        float: Converted value
        
    Raises:
        UnitConversionError: If unit systems are incompatible or conversion fails
        ValueError: If input parameters are invalid
    """
    if standard_unit_system != UCUM_SYSTEM:
        raise ValueError(f"Unsupported standard unit system: {standard_unit_system}. Must be {UCUM_SYSTEM}.")

    if not isinstance(value, (int, float)):
        raise ValueError(f"Value must be numeric, got {type(value)}: {value}")

    # If units are the same, no conversion needed
    if input_unit == standard_unit:
        logger.debug(f"No conversion needed: {input_unit} == {standard_unit}")
        return float(value)

    try:
        ureg = UnitRegistry()
                
        # Map input and target units to Pint units
        pint_input_unit = UNIT_MAPPING.get(input_unit)
        pint_standard_unit = UNIT_MAPPING.get(standard_unit)
        
        # Validate that both units are supported
        if pint_input_unit is None:
            raise UnitConversionError(f"Unsupported input unit: '{input_unit}'. Supported units: {list(UNIT_MAPPING.keys())}")
        if pint_standard_unit is None:
            raise UnitConversionError(f"Unsupported standard unit: '{standard_unit}'. Supported units: {list(UNIT_MAPPING.keys())}")
        
        logger.debug(f"Converting {value} {input_unit} ({pint_input_unit}) to {standard_unit} ({pint_standard_unit})")
        
        # Create quantity and convert using explicit constructor
        quantity = ureg.Quantity(value, pint_input_unit)
        converted_quantity = quantity.to(pint_standard_unit)
        
        result = float(converted_quantity.magnitude)
        logger.debug(f"Conversion result: {value} {input_unit} = {result} {standard_unit}")
        
        return result
        
    except ImportError as e:
        error_msg = "Pint library not available. Install with: pip install pint"
        logger.error(error_msg)
        raise UnitConversionError(error_msg) from e
        
    except Exception as e:
        error_msg = f"Unit conversion failed: {value} {input_unit} -> {standard_unit}. Error: {e}"
        logger.error(error_msg)
        raise UnitConversionError(error_msg) from e