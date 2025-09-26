import pytest
from unittest.mock import patch
from typing import cast
from fhir import DiagnosticsService, TerminologyService
from fhirclient.models.coding import Coding
from fhirclient.models.observationdefinition import ObservationDefinition
from fhirclient.models.fhirabstractbase import FHIRValidationError
from fhirclient import client

# ------------------------
# Fixtures & Helpers
# ------------------------

@pytest.fixture
def mock_observation_definition(monkeypatch):
    class MockObsDef:
        class QuantitativeDetails:
            permittedUnits = [type("Unit", (), {"code": "cm"})]
        quantitativeDetails = QuantitativeDetails()
    monkeypatch.setattr("fhir.ObservationDefinition", MockObsDef)
    return MockObsDef()

@pytest.fixture
def mock_coding(monkeypatch):
    class MockCoding:
        def __init__(self, code="CODE", system="SYSTEM", display="DISPLAY"):
            self.code = code
            self.system = system
            self.display = display
        def as_json(self):
            return {"code": self.code, "system": self.system, "display": self.display}
    monkeypatch.setattr("fhir.Coding", MockCoding)
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
        self.server = server or MockFHIRServer()

# ------------------------
# DiagnosticsService Tests
# ------------------------

def test_get_observation_definition_cached(mock_observation_definition):
    DiagnosticsService._cache = {"TEST_CODE": mock_observation_definition}
    result = DiagnosticsService.get_observation_definition("TEST_CODE")
    assert result is mock_observation_definition

def test_get_observation_definition_none_code():
    DiagnosticsService._cache = {}
    with pytest.raises(TypeError, match="code must not be None or empty"):
        DiagnosticsService.get_observation_definition(cast(str, None))    

def test_get_observation_definition_empty_code():
    DiagnosticsService._cache = {}
    with pytest.raises(TypeError, match="code must not be None or empty"):
        DiagnosticsService.get_observation_definition("")

def test_get_observation_definition_not_found(monkeypatch):
    DiagnosticsService._cache = {}

    class MockServer:
      def request_json(self, url: str):
        return None

    class MockClient(client.FHIRClient):
        def __init__(self):
            self.server = MockServer()

    class MockSearch:
        def perform_resources(self, server):
            # Simulate no results found
            return []

    #with patch("fhirclient.models.observationdefinition.ObservationDefinition.where", return_value=MockSearch()):
    result = DiagnosticsService.get_observation_definition("NOT_FOUND", MockFHIRClient())

    assert result is None

# ------------------------
# TerminologyService Tests
# ------------------------

def test_translate_valid(monkeypatch):
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

    DiagnosticsService._cache = {}
    with pytest.raises(TypeError, match="code must not be None or empty"):
        DiagnosticsService.get_observation_definition(cast(str, None))       

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
    result = TerminologyService.translate("UNKNOWN", "http://loinc.org", MockFHIRClient(MockFHIRServer()))
    assert result is None

def test_translate_malformed_result(monkeypatch):
    server = MockFHIRServer({
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
    })

    result = TerminologyService.translate("LOINC_HEIGHT", "http://loinc.org", MockFHIRClient(server))
    assert result is None

def test_translate_server_unavailable(monkeypatch):
    pass
    # TODO - how to simulate server unavailability?

def test_to_coding_with_coding_instance(mock_coding):
    coding_instance = mock_coding("CODE", "SYSTEM", "DISPLAY")
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
        TerminologyService._to_coding(None)         