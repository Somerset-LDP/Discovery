from typing import Optional, ClassVar, Dict, List
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
from urllib.parse import quote

logging.basicConfig(
    filename='logs/diagnostic_service.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def _get_fhir_client(api_base: str = "", app_id: str = "diagnostics-service") -> client.FHIRClient:
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
    def _get_all_observation_definitions(cls, client: Optional[client.FHIRClient] = None) -> List[ObservationDefinition]:
        obs_defs: list[ObservationDefinition] = []

        # Use injected client or default
        client = client or _get_fhir_client()
        server = client.server
        
        if server:
            path = f"ObservationDefinition?_search"
            logging.debug(f"Calling $translate at path {path}")
            
            result = server.request_json(path)     

            if result and result.get("resourceType") == "Bundle":
                for entry in result.get("entry", []):
                    resource = entry.get("resource")
                    if resource and resource.get("resourceType") == "ObservationDefinition":
                        try:
                            obs_def = ObservationDefinition(resource)
                            obs_defs.append(obs_def)
                        except Exception as e:
                            logging.error(f"Failed to parse ObservationDefinition: {e}")    

        return obs_defs  

    @classmethod
    def _cache_observation_definitions(cls, obs_defs: List[ObservationDefinition]):
        for od in obs_defs:
            if od.id:
                cls._cache[od.id] = od
            else:
                logging.warning(f"Could not cache ObservationDefinition with missing id: {od.as_json()}")
         
    @classmethod
    def get_observation_definition(cls, code: str, system: str, client: Optional[client.FHIRClient] = None) -> Optional[ObservationDefinition]:
        """
        Looks up an ObservationDefinition in the cache, populating the cache if empty.
        Returns None if not found.
        """
        # Populate cache if empty
        if not cls._cache:
            obs_defs = cls._get_all_observation_definitions(client)
            cls._cache_observation_definitions(obs_defs)

        # Lookup in cache  
        for od in cls._cache.values():
            if od.code and od.code.coding:
                for coding in od.code.coding: # type: ignore
                    if coding.system == system and coding.code == code:
                        return od

        # Not found
        return None
    
    @classmethod
    def get_observation_definition_by_id(cls, id: str, client: Optional[client.FHIRClient] = None) -> Optional[ObservationDefinition]:
        """
        Looks up an ObservationDefinition in the cache, populating the cache if empty.
        Returns None if not found.
        """
        # Populate cache if empty
        if not cls._cache:
            obs_defs = cls._get_all_observation_definitions(client)
            cls._cache_observation_definitions(obs_defs)

        print(f"Cache contains {len(cls._cache)} ObservationDefinitions.")

        # Lookup in cache
        return cls._cache.get(id)
