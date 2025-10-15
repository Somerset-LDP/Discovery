import logging
from datetime import datetime, date
from rcpchgrowth import Measurement
from fhirclient import client
from fhir.diagnostic_service import DiagnosticsService
from typing import Optional, List, Tuple, TypedDict, Callable, Any

# Get a logger for this module
logger = logging.getLogger(__name__)

class Code(TypedDict):
    code: str
    system: str

def _is_adult(dob: date, today: date = date.today()) -> bool:
    """
    Determine if a person is 18 or over based on their date of birth.
    
    Args:
        dob: Date of birth
        today: Current date (defaults to today if not provided)
    
    Returns:
        True if person is 18 or over, False otherwise
    """    
    # Calculate age by comparing years, then adjusting for month/day
    age = today.year - dob.year
    
    # If birthday hasn't occurred this year, subtract 1
    if (today.month, today.day) < (dob.month, dob.day):
        age -= 1
    
    return age >= 18
                     
def _ethnicity_matches(ethnicities: List, target: Code) -> bool:
    """
    Check if the ethnicity matches any of the appliesTo codings.
    """
    for ethnicity in ethnicities:
        if ethnicity.coding:
            for coding in ethnicity.coding:
                if (coding.code == target.get("code") and 
                    coding.system == target.get("system")):
                    return True
    return False

def _in_range(target: float, range_obj) -> bool:
    """
    Check if BMI falls within the specified range.
    """
    if not range_obj:
        return False
    
    # Check lower bound
    if range_obj.low and hasattr(range_obj.low, 'value'):
        if target < range_obj.low.value:
            return False
    
    # Check upper bound (if it exists)
    if range_obj.high and hasattr(range_obj.high, 'value'):
        if target > range_obj.high.value:
            return False
    
    return True  

def _get_category(qualifiedInterval) -> Optional[Code]:
    if qualifiedInterval.context and qualifiedInterval.context.coding:
        for coding in qualifiedInterval.context.coding:
            if coding.code and coding.system:
                return Code(code=coding.code, system=coding.system)
    return None

def _determine_weight_category(
    target_value: float, 
    obs_def_id: str, 
    interval_filter: Callable[[Any], bool],
    fhir_client: Optional[client.FHIRClient] = None
) -> Optional[Code]:
    """
    Common logic for determining weight categories from ObservationDefinition.
    
    Args:
        target_value: The value to match against ranges (BMI or centile)
        obs_def_id: ID of the ObservationDefinition to lookup
        interval_filter: Lambda function to filter which intervals apply
        fhir_client: FHIR client for lookups
    
    Returns:
        Code object if match found, None otherwise
    """
    # Lookup the ObservationDefinition
    obs_def = DiagnosticsService.get_observation_definition_by_id(obs_def_id, fhir_client)

    if not obs_def or not obs_def.qualifiedInterval:
        logger.error(f"ObservationDefinition '{obs_def_id}' not found or it is incomplete.")
        return None
    
    # Iterate through each qualified interval
    for interval in obs_def.qualifiedInterval: # type: ignore
        # Apply the custom filter (ethnicity matching for adults, no filter for children)
        if interval_filter(interval):
            # Check if value falls within this interval's range
            if _in_range(target_value, interval.range):
                # Extract and return the category code from context
                return _get_category(interval)
    
    return None

def _map_snomed_sex_to_rcpchgrowth_sex(snomed_code: str) -> Optional[str]:
    sex_mapping = {
        "248152002": "female",   
        "248153007": "male"     
    }

    return sex_mapping.get(snomed_code)

def _determine_adult_weight_category(bmi: float, ethnicity: Code, fhir_client: Optional[client.FHIRClient] = None) -> Optional[Code]:
    return _determine_weight_category(
        target_value=bmi,
        obs_def_id="adult-who-bmi-clinical-classification",
        interval_filter=lambda interval: interval.appliesTo and _ethnicity_matches(interval.appliesTo, ethnicity),
        fhir_client=fhir_client
    )

def _determine_child_weight_category(bmi: float, dob: date, sex: Code, fhir_client: Optional[client.FHIRClient] = None) -> Optional[Code]:
    category: Optional[Code] = None
    
    # make sure that the sex is coded in snomed
    if sex.get("system") != "http://snomed.info/sct":
        raise ValueError("Sex code must be SNOMED CT")
    
    # now convert to sex as used by rcpchgrowth
    rcpchgrowth_sex = _map_snomed_sex_to_rcpchgrowth_sex(sex.get("code"))
    if not rcpchgrowth_sex:
        logger.warning(f"Unable to calculate weight category. Unknown SNOMED sex code: {sex.get('code')}")
        return category

    # Calculate centile first
    measurement = Measurement(
        birth_date=dob, 
        sex=rcpchgrowth_sex, 
        measurement_method="bmi", 
        reference="uk-who", 
        observation_value=bmi, 
        observation_date=datetime.now().date()
    )
    centile = measurement.measurement["measurement_calculated_values"]["chronological_centile"]

    category = _determine_weight_category(
        target_value=centile,
        obs_def_id="child-uk90-bmi-clinical-classification", 
        interval_filter=lambda interval: True,  # No additional filtering for children
        fhir_client=fhir_client
    )

    return category

def calculate_bmi_and_category(patient, fhir_client: Optional[client.FHIRClient] = None) -> Tuple[Optional[float], Optional[Code]]:

    dob = patient["dob"]
    height_cm = patient["height_cm"]
    weight_kg = patient["weight_kg"]
    ethnicity = Code(
        code=patient["ethnicity_code"],
        system=patient["ethnicity_code_system"]
    )
    sex = Code(
        code=patient["sex_code"],
        system=patient["sex_code_system"]
    )

    if height_cm and weight_kg:
        bmi = weight_kg / ((height_cm / 100) ** 2)    

        category = None
        if(_is_adult(dob)):
            category = _determine_adult_weight_category(bmi, ethnicity, fhir_client)
        else:
            category = _determine_child_weight_category(bmi, dob, sex, fhir_client)

        return bmi, category
    else:
        return None, None