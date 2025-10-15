import pytest
from unittest.mock import patch
from typing import cast
from fhir.diagnostic_service import DiagnosticsService
from fhir.terminology_service import TerminologyService
from fhirclient.models.coding import Coding
from fhirclient.models.observationdefinition import ObservationDefinition
from fhirclient.models.fhirabstractbase import FHIRValidationError
from fhirclient import client

# ------------------------
# Fixtures & Helpers
# ------------------------

@pytest.fixture
def mock_observation_definition():
    """Create a mock ObservationDefinition for testing."""
    class MockObsDef:
        def __init__(self):
            self.id = "test-obs-def"
            self.code = type("CodeableConcept", (), {
                "coding": [type("Coding", (), {
                    "code": "test-code",
                    "system": "http://loinc.org"
                })]
            })
        
        class QuantitativeDetails:
            permittedUnits = [type("Unit", (), {"code": "cm"})]
        quantitativeDetails = QuantitativeDetails()
    return MockObsDef()

@pytest.fixture
def mock_coding():
    """Create a mock Coding class for testing."""
    class MockCoding:
        def __init__(self, code="CODE", system="SYSTEM", display="DISPLAY"):
            self.code = code
            self.system = system
            self.display = display
        def as_json(self):
            return {"code": self.code, "system": self.system, "display": self.display}
    return MockCoding

class MockFHIRServer:
    """Mock FHIR server with base_uri and request_json."""
    base_uri = "http://mock-fhir-server"

    def __init__(self, translate_response=None):
        """
        translate_response: dict returned by request_json for $translate
        Defaults to empty JSON.
        """
        self._translate_response = translate_response

    def request_json(self, url):
        return self._translate_response


class MockFHIRClient(client.FHIRClient):
    """Mock FHIR client with a server."""
    def __init__(self, server=None):
        # Don't call parent __init__ to avoid server initialization issues
        self.server = server or MockFHIRServer()

# ------------------------
# DiagnosticsService Tests
# ------------------------

def test_get_observation_definition_cached(mock_observation_definition):
    DiagnosticsService._cache = {"test-obs-def": mock_observation_definition}
    result = DiagnosticsService.get_observation_definition("test-code", "http://loinc.org")
    assert result is mock_observation_definition

def test_get_observation_definition_none_code(monkeypatch):
    DiagnosticsService._cache = {}
    
    # Mock the _get_fhir_client function
    def mock_get_fhir_client():
        mock_server = type('MockServer', (), {
            'request_json': lambda self, url: {"resourceType": "Bundle", "entry": []}
        })()
        return type('MockClient', (), {'server': mock_server})()

    monkeypatch.setattr("fhir.diagnostic_service._get_fhir_client", mock_get_fhir_client)
    
    result = DiagnosticsService.get_observation_definition(cast(str, None), "http://loinc.org")
    # The method doesn't validate None code, it just won't find anything
    assert result is None    

def test_get_observation_definition_empty_code(monkeypatch):
    DiagnosticsService._cache = {}
    
    # Mock the _get_fhir_client function
    def mock_get_fhir_client():
        mock_server = type('MockServer', (), {
            'request_json': lambda self, url: {"resourceType": "Bundle", "entry": []}
        })()
        return type('MockClient', (), {'server': mock_server})()

    monkeypatch.setattr("fhir.diagnostic_service._get_fhir_client", mock_get_fhir_client)
    
    result = DiagnosticsService.get_observation_definition("", "http://loinc.org")
    # The method doesn't validate empty code, it just won't find anything
    assert result is None

def test_get_observation_definition_not_found(monkeypatch):
    DiagnosticsService._cache = {}

    # Mock the _get_fhir_client function to return a mock client
    def mock_get_fhir_client():
        mock_server = type('MockServer', (), {
            'base_uri': 'http://mock-server',
            'request_json': lambda self, url: {
                "resourceType": "Bundle", 
                "entry": []
            }
        })()
        
        mock_client = type('MockClient', (), {'server': mock_server})()
        return mock_client

    monkeypatch.setattr("fhir.diagnostic_service._get_fhir_client", mock_get_fhir_client)
    
    result = DiagnosticsService.get_observation_definition("NOT_FOUND", "http://loinc.org")
    assert result is None

# ------------------------
# TerminologyService Tests
# ------------------------

def test_translate_valid():
    server = MockFHIRServer({
        "parameter": [
            {
                "name": "match",
                "part": [
                    {
                        "name": "concept",
                        "valueCoding": {
                            "code": "CODE",
                            "system": "http://snomed.info/sct",
                            "display": "Height"
                        }
                    }
                ]
            }
        ]
    })

    result = TerminologyService.translate("LOINC_HEIGHT", "http://loinc.org", MockFHIRClient(server))
    assert isinstance(result, Coding)
    assert result.code == "CODE"
    assert result.system == "http://snomed.info/sct"       

def test_translate_none_code():
    with pytest.raises(TypeError, match="both code and system must not be None or empty"):
        TerminologyService.translate(cast(str, None), "http://loinc.org")

def test_translate_none_system():
    with pytest.raises(TypeError, match="both code and system must not be None or empty"):
        TerminologyService.translate("LOINC_HEIGHT", cast(str, None))

def test_translate_empty_code():
    with pytest.raises(TypeError, match="both code and system must not be None or empty"):
        TerminologyService.translate("", "http://loinc.org")    

def test_translate_empty_system():
    with pytest.raises(TypeError, match="both code and system must not be None or empty"):
        TerminologyService.translate("LOINC_HEIGHT", "")            

# MockFHIRClient(server)
def test_translate_not_found(monkeypatch):
    # Mock the _get_fhir_client function to return a client that returns None
    def mock_get_fhir_client():
        mock_server = type('MockServer', (), {
            'request_json': lambda self, url: None  # No translation found
        })()
        return type('MockClient', (), {'server': mock_server})()

    monkeypatch.setattr("fhir.terminology_service.TerminologyService._get_fhir_client", mock_get_fhir_client)
    
    result = TerminologyService.translate("UNKNOWN", "http://loinc.org")
    assert result is None

def test_translate_malformed_result(monkeypatch):
    # Mock the _get_fhir_client function to return malformed result
    def mock_get_fhir_client():
        mock_server = type('MockServer', (), {
            'request_json': lambda self, url: {
                "parameter": [
                    {
                        "name": "match",
                        "part": [
                            {
                                "name": "concept",
                                "valueCoding": {} # empty coding
                            }
                        ]
                    }
                ]
            }
        })()
        return type('MockClient', (), {'server': mock_server})()

    monkeypatch.setattr("fhir.terminology_service.TerminologyService._get_fhir_client", mock_get_fhir_client)
    
    result = TerminologyService.translate("LOINC_HEIGHT", "http://loinc.org")
    assert result is None

def test_translate_server_unavailable(monkeypatch):
    pass
    # TODO - how to simulate server unavailability?

def test_to_coding_with_coding_instance():
    # Create an actual Coding instance for testing
    coding_instance = Coding({"code": "CODE", "system": "SYSTEM", "display": "DISPLAY"})
    result = TerminologyService._to_coding(coding_instance)
    assert result is coding_instance

def test_to_coding_with_dict_all_keys():
    data = {"code": "CODE", "system": "SYSTEM", "display": "DISPLAY"}
    result = TerminologyService._to_coding(data)
    assert isinstance(result, Coding)
    assert result.code == "CODE"
    assert result.system == "SYSTEM"
    assert result.display == "DISPLAY"

def test_to_coding_with_dict_missing_keys():
    data = {"code": "CODE"}
    result = TerminologyService._to_coding(data)
    assert result is None

def test_to_coding_with_invalid_object():
    class Dummy: pass
    result = TerminologyService._to_coding(Dummy())
    assert result is None

def test_to_coding_with_none():
    with pytest.raises(TypeError, match="data must not be None or empty"):
        TerminologyService._to_coding(None) 

def test_to_coding_with_empty():
    with pytest.raises(TypeError, match="data must not be None or empty"):
        TerminologyService._to_coding("")         