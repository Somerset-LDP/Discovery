from typing import Optional, ClassVar, Dict
from fhirclient import client
from fhirclient.models.observationdefinition import ObservationDefinition
from fhirclient.models.observation import Observation
from fhirclient.models.fhirabstractbase import FHIRValidationError
from fhirclient.models import parameters as fhir_parameters
from fhirclient.models.coding import Coding
from fhirclient.models.quantity import Quantity
from fhirclient.models.codeableconcept import CodeableConcept
from datetime import datetime, time
import logging

logging.basicConfig(
    filename='logs/ingestion_errors.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def _get_fhir_client(api_base: str = "", app_id: str = "observation_definition_cache") -> client.FHIRClient:
    settings = {
        "app_id": app_id,
        "api_base": api_base,
    }
    return client.FHIRClient(settings=settings)

# Diagnostics module - https://build.fhir.org/diagnostics-module.html

class DiagnosticsService:
    """Singleton cache for ObservationDefinitions fetched from FHIR server by code."""
    _cache: ClassVar[Dict[str, ObservationDefinition]] = {}

    @classmethod
    def get_observation_definition(cls, code: str, client: Optional[client.FHIRClient] = None) -> Optional[ObservationDefinition]:
        """Retrieve ObservationDefinition by code, fetching from server if not cached."""
        if not code:
            raise TypeError("code must not be None or empty")        
        
        if code in cls._cache:
            return cls._cache[code]

        # Use injected client or default
        client = client or _get_fhir_client()
        server = client.server

        # Fetch the ObservationDefinition for this code
        search = ObservationDefinition.where(struct={'code': code})
        results = search.perform_resources(server)

        if results:
            # Cast/instantiate resource as ObservationDefinition
            try:
                od = ObservationDefinition(results[0].as_json())
                cls._cache[code] = od
                return od
            except FHIRValidationError:
                # If the resource cannot be cast, skip caching
                return None

        return None
    
# Terminology module - https://build.fhir.org/terminology-module.html

class TerminologyService:

    @classmethod
    def translate(cls, code: str, system: str, client: Optional[client.FHIRClient] = None) -> Optional[Coding]:
        """
        Calls ConceptMap/$translate.
        Returns the first translated Coding if found, otherwise None.
        """
        if not code or not system:
            raise TypeError("both code and system must not be None or empty")    
        
        translation: Optional[Coding] = None

        # Use injected client or default
        client = client or _get_fhir_client()
        server = client.server

        if server:
            url = f"{server.base_uri}/ConceptMap/$translate?code={code}&system={system}"

            logging.debug(f"Calling $translate at {url}")
            result = server.request_json(url)

            if result:
                parameters = fhir_parameters.Parameters(result)
                if parameters and parameters.parameter:
                    # Find first match
                    match_params = [
                        p for p in parameters.parameter  # type: ignore
                        if getattr(p, "name", None) == "match"
                    ]

                    for match in match_params:
                        for part in getattr(match, "part", []):
                            if getattr(part, "name", None) == "concept" and getattr(part, "valueCoding", None):
                                translation = cls._to_coding(part.valueCoding)

        return translation
    
    @classmethod
    def _to_coding(cls, data) -> Optional[Coding]:
        if not data:
            raise TypeError("data must not be None or empty")     
        
        coding: Optional[Coding] = None
        
        if isinstance(data, Coding):
            coding = data
        else:
            try:
                coding = Coding(data.as_json())
            except Exception:
                if isinstance(data, dict):
                    coding = Coding({
                        "system": data.get("system"),
                        "code": data.get("code"),
                        "display": data.get("display"),
                    })
                else:
                    logging.error(f"Invalid data type for Coding conversion: {type(data)}")
                    coding = None
                    
        if coding:
            if not coding.system or not coding.code:
                logging.error(f"Invalid Coding: missing 'system' or 'code' in {coding.as_json()}")
                coding = None

        return coding                

