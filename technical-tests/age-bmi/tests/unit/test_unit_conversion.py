#!/usr/bin/env python3
"""
Test script to demonstrate Pint unit conversion with the raw_patients.json data
"""

def test_unit_conversion():
    """Demonstrate unit conversion for each patient in raw_patients.json"""
    
    try:
        from pint import UnitRegistry
        ureg = UnitRegistry()
        print("✅ Pint library available")
    except ImportError:
        print("❌ Pint not installed. Run: pip install pint")
        return
    
    # Unit mapping as used in pipeline
    UNIT_MAPPING = {
        'inches': 'inch',
        'in': 'inch', 
        'cm': 'centimeter',
        'm': 'meter',
        'lbs': 'pound',
        'lb': 'pound',
        'kg': 'kilogram',
    }
    
    # Test cases from raw_patients.json
    test_cases = [
        # Patient 1 - LOINC codes with imperial units (needs conversion)
        {"patient": 1, "type": "29463-7", "system": "http://loinc.org", "value": 154, "unit": "lbs", "target_unit": "kg", "should_convert": True},
        {"patient": 1, "type": "8302-2", "system": "http://loinc.org", "value": 65, "unit": "inches", "target_unit": "cm", "should_convert": True},
        
        # Patient 2 - SNOMED codes with metric units (no conversion needed)
        {"patient": 2, "type": "27113001", "system": "http://snomed.info/sct", "value": 75, "unit": "kg", "target_unit": "kg", "should_convert": False},
        {"patient": 2, "type": "50373000", "system": "http://snomed.info/sct", "value": 172, "unit": "cm", "target_unit": "cm", "should_convert": False},
        
        # Patient 3 - SNOMED codes with metric units (no conversion needed)
        {"patient": 3, "type": "27113001", "system": "http://snomed.info/sct", "value": 35, "unit": "kg", "target_unit": "kg", "should_convert": False},
        {"patient": 3, "type": "50373000", "system": "http://snomed.info/sct", "value": 140, "unit": "cm", "target_unit": "cm", "should_convert": False},
        
        # Patient 4 - LOINC codes with imperial units (needs conversion)  
        {"patient": 4, "type": "29463-7", "system": "http://loinc.org", "value": 58.8, "unit": "lbs", "target_unit": "kg", "should_convert": True},
        {"patient": 4, "type": "8302-2", "system": "http://loinc.org", "value": 47.2, "unit": "inches", "target_unit": "cm", "should_convert": True},
    ]
    
    print("\n🧪 Unit Conversion Test Results")
    print("=" * 80)
    
    for case in test_cases:
        patient = case["patient"]
        obs_type = case["type"]
        system = case["system"] 
        value = case["value"]
        input_unit = case["unit"]
        target_unit = case["target_unit"]
        should_convert = case["should_convert"]
        
        print(f"\n👤 Patient {patient} | {obs_type} ({system.split('/')[-1]})")
        print(f"   Input: {value} {input_unit} → Target: {target_unit}")
        
        try:
            if input_unit == target_unit:
                converted_value = value
                print(f"   ✅ No conversion needed: {converted_value} {target_unit}")
            else:
                # Perform conversion
                pint_input_unit = UNIT_MAPPING.get(input_unit, input_unit)
                pint_target_unit = UNIT_MAPPING.get(target_unit, target_unit) 
                
                quantity = value * ureg(pint_input_unit) # type: ignore
                converted_quantity = quantity.to(pint_target_unit)
                converted_value = float(converted_quantity.magnitude)
                
                print(f"   🔄 Converted: {converted_value:.2f} {target_unit}")
                
                # Show BMI calculation for height/weight pairs
                if target_unit == "kg":
                    print(f"   📊 Weight for BMI: {converted_value:.1f} kg")
                elif target_unit == "cm":
                    height_m = converted_value / 100
                    print(f"   📏 Height for BMI: {height_m:.2f} m")
                    
        except Exception as e:
            print(f"   ❌ Conversion failed: {e}")

    print(f"\n🧮 BMI Preview Calculations")
    print("=" * 40)
    
    # Show what BMI calculations would look like
    patients_data = [
        {"id": 1, "weight_lbs": 154, "height_in": 65},
        {"id": 2, "weight_kg": 75, "height_cm": 172}, 
        {"id": 3, "weight_kg": 35, "height_cm": 140},
        {"id": 4, "weight_lbs": 58.8, "height_in": 47.2}
    ]
    
    for patient in patients_data:
        patient_id = patient["id"]
        
        try:
            if "weight_lbs" in patient:
                # Convert imperial to metric
                weight_kg = (patient["weight_lbs"] * ureg.pound).to("kilogram").magnitude
                height_m = (patient["height_in"] * ureg.inch).to("meter").magnitude
            else:
                # Already metric
                weight_kg = patient["weight_kg"] 
                height_m = patient["height_cm"] / 100
                
            bmi = weight_kg / (height_m ** 2)
            print(f"Patient {patient_id}: BMI = {bmi:.1f} (Weight: {weight_kg:.1f}kg, Height: {height_m:.2f}m)")
            
        except Exception as e:
            print(f"Patient {patient_id}: BMI calculation failed: {e}")

if __name__ == "__main__":
    test_unit_conversion()