from typing import Optional
from fhirclient import client
from fhirclient.models import parameters as fhir_parameters
from fhirclient.models.coding import Coding
from urllib.parse import quote
import logging

# Get a logger for this module
logger = logging.getLogger(__name__)

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
        client = client or cls._get_fhir_client()
        server = client.server

        if server:
            encoded_code = quote(code, safe='')
            encoded_system = quote(system, safe='')
            path = f"ConceptMap/$translate?code={encoded_code}&system={encoded_system}"
            logger.debug(f"Calling $translate at path {path}")
            
            result = server.request_json(path)

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
                    logger.error(f"Invalid data type for Coding conversion: {type(data)}")
                    coding = None
                    
        if coding:
            if not coding.system or not coding.code:
                logger.error(f"Invalid Coding: missing 'system' or 'code' in {coding.as_json()}")
                coding = None

        return coding                  

    @classmethod
    def _get_fhir_client(cls, api_base: str = "", app_id: str = "terminology-service") -> client.FHIRClient:
        settings = {
            "app_id": app_id,
            "api_base": api_base,
        }
        return client.FHIRClient(settings=settings)

    from urllib.parse import urlparse      