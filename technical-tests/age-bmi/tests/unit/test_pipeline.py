import pytest
import pandas as pd
import os
from pipeline import (
    read_raw_input,
    transform_to_refined_patients,
    write_raw_patients,
    load_raw_patients_from_datalake,
    map_to_snomed,
    convert_value_to_standard_unit,
    standardise_observation,
    standardise_patient_observations,
    write_refined_patients,
)

# ------------------------
# Fixtures & Helpers
# ------------------------

@pytest.fixture
def valid_patient():
    return {
        "patient_id": 1,
        "dob": "2000-01-01",
        "observations": [
            {
                "type": "LOINC_HEIGHT",
                "type_code_system": "http://loinc.org",
                "value": 180,
                "unit": "cm",
                "observation_time": "2023-01-01T10:00:00Z"
            },
            {
                "type": "LOINC_WEIGHT",
                "type_code_system": "http://loinc.org",
                "value": 75,
                "unit": "kg",
                "observation_time": "2023-01-01T10:00:00Z"
            }
        ]
    }

@pytest.fixture
def mock_snomed_codes(monkeypatch):
    monkeypatch.setenv("SNOMED_BODY_HEIGHT", "SNOMED_HEIGHT_CODE")
    monkeypatch.setenv("SNOMED_BODY_WEIGHT", "SNOMED_WEIGHT_CODE")

@pytest.fixture
def mock_terminology(monkeypatch):
    class MockCoding:
        def __init__(self, code, system):
            self.code = code
            self.system = system
    def mock_translate(code, system):
        if code == "LOINC_HEIGHT":
            return MockCoding("SNOMED_HEIGHT_CODE", "http://snomed.info/sct")
        if code == "LOINC_WEIGHT":
            return MockCoding("SNOMED_WEIGHT_CODE", "http://snomed.info/sct")
        return None
    monkeypatch.setattr("pipeline.TerminologyService.translate", mock_translate)

@pytest.fixture
def mock_diagnostics(monkeypatch):
    class MockObsDef:
        class QuantitativeDetails:
            permittedUnits = [type("Unit", (), {"code": "cm"})]
        quantitativeDetails = QuantitativeDetails()
    def mock_get_observation_definition(code):
        return MockObsDef() if code in ["SNOMED_HEIGHT_CODE", "SNOMED_WEIGHT_CODE"] else None
    monkeypatch.setattr("pipeline.DiagnosticsService.get_observation_definition", mock_get_observation_definition)

@pytest.fixture
def mock_db(monkeypatch):
    class MockEngine:
        def __init__(self, *args, **kwargs): pass
    def mock_create_engine(db_url): return MockEngine()
    def mock_to_sql(self, table_name, engine, if_exists, index): return None
    monkeypatch.setattr("pipeline.create_engine", mock_create_engine)
    monkeypatch.setattr(pd.DataFrame, "to_sql", mock_to_sql)

# ------------------------
# Happy Path Tests
# ------------------------

def test_read_raw_input_validates_successfully(valid_patient, tmp_path, monkeypatch):
    schema_path = tmp_path / "schema-inbound.json"
    schema_path.write_text("""
    {
        "type": "object",
        "properties": {
            "patient_id": {"type": "integer"},
            "dob": {"type": "string"},
            "observations": {"type": "array"}
        },
        "required": ["patient_id", "dob"]
    }
    """)
    
    patients = read_raw_input([valid_patient], schema_path)
    assert len(patients) == 1
    assert patients[0]["patient_id"] == 1

def test_standardise_observation_happy_path(valid_patient, mock_terminology, mock_diagnostics):
    obs = valid_patient["observations"][0]
    standardised = standardise_observation(obs)
    assert standardised is not None
    assert standardised["type_code_system"] == "http://snomed.info/sct"

def test_transform_to_refined_patients_happy_path(valid_patient, mock_snomed_codes, mock_terminology, mock_diagnostics):
    enriched = standardise_patient_observations([valid_patient])
    df = transform_to_refined_patients(enriched)
    assert "height_cm" in df.columns
    assert "weight_kg" in df.columns
    assert "bmi" in df.columns
    assert df.iloc[0]["height_cm"] == 180
    assert df.iloc[0]["weight_kg"] == 75
    assert df.iloc[0]["bmi"] == pytest.approx(75 / ((180 / 100) ** 2))

def test_write_refined_patients_happy_path(valid_patient, mock_snomed_codes, mock_terminology, mock_diagnostics, mock_db, monkeypatch):
    enriched = standardise_patient_observations([valid_patient])
    df = transform_to_refined_patients(enriched)
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    write_refined_patients(df)  # Should not raise

# ------------------------
# Unhappy Path Tests
# ------------------------

def test_missing_snomed_env_vars(valid_patient, monkeypatch):
    monkeypatch.delenv("SNOMED_BODY_HEIGHT", raising=False)
    monkeypatch.delenv("SNOMED_BODY_WEIGHT", raising=False)
    with pytest.raises(EnvironmentError):
        transform_to_refined_patients([valid_patient])

def test_missing_database_url(valid_patient, mock_snomed_codes, mock_terminology, mock_diagnostics):
    enriched = standardise_patient_observations([valid_patient])
    df = transform_to_refined_patients(enriched)
    if "DATABASE_URL" in os.environ:
        del os.environ["DATABASE_URL"]
    with pytest.raises(EnvironmentError):
        write_refined_patients(df)

def test_standardise_observation_missing_mapping(valid_patient, mock_terminology, mock_diagnostics):
    obs = dict(valid_patient["observations"][0])
    obs["type"] = "UNKNOWN_TYPE"
    standardised = standardise_observation(obs)
    assert standardised is None

def test_standardise_observation_missing_observation_definition(valid_patient, mock_terminology, monkeypatch):
    def mock_get_observation_definition(code): return None
    monkeypatch.setattr("pipeline.DiagnosticsService.get_observation_definition", mock_get_observation_definition)
    obs = valid_patient["observations"][0]
    standardised = standardise_observation(obs)
    assert standardised is None

def test_transform_to_refined_patients_missing_height_or_weight(valid_patient, mock_snomed_codes, mock_terminology, mock_diagnostics):
    enriched = standardise_patient_observations([valid_patient])
    # Remove weight observation
    enriched[0]["observations"] = [obs for obs in enriched[0]["observations"] if obs.get("type") != os.getenv("SNOMED_BODY_WEIGHT")]
    df = transform_to_refined_patients(enriched)
    assert df.iloc[0]["bmi"] is None