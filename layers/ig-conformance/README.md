from abc import ABC, abstractmethod

class PseudonymisationService(ABC):
    """Abstract interface for pseudonymisation services"""
    
    @abstractmethod
    def pseudonymise_nhs_number(self, nhs_number: str) -> str:
        """
        Pseudonymise an NHS number
        
        Args:
            nhs_number (str): The NHS number to pseudonymise
            
        Returns:
            str: The pseudonymised NHS number
            
        Raises:
            PseudonymisationError: If pseudonymisation fails
        """
        pass

class PseudonymisationError(Exception):
    """Raised when pseudonymisation fails"""
    pass

import boto3
import json
import logging
from typing import Optional
from botocore.exceptions import ClientError, BotoCoreError
from common.pseudonymisation import PseudonymisationService, PseudonymisationError

class AWSLambdaPseudonymisationService(PseudonymisationService):
    """AWS Lambda implementation of pseudonymisation service"""
    
    def __init__(self, function_name: str, region_name: str = "eu-west-2"):
        """
        Initialize the AWS Lambda pseudonymisation service
        
        Args:
            function_name (str): Name of the Lambda function
            region_name (str): AWS region name (defaults to eu-west-2)
        """
        self.function_name = function_name
        self.lambda_client = boto3.client('lambda', region_name=region_name)
        self.logger = logging.getLogger(__name__)
    
    def pseudonymise_nhs_number(self, nhs_number: str) -> str:
        """
        Call AWS Lambda function to pseudonymise NHS number
        
        Args:
            nhs_number (str): The NHS number to pseudonymise
            
        Returns:
            str: The pseudonymised NHS number
            
        Raises:
            PseudonymisationError: If Lambda invocation fails or returns error
        """
        try:
            payload = {
                "nhs_number": nhs_number
            }
            
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType='RequestResponse',  # Synchronous call
                Payload=json.dumps(payload)
            )
            
            # Parse the response
            response_payload = json.loads(response['Payload'].read())
            
            # Check if Lambda function returned an error
            if response.get('FunctionError'):
                error_msg = response_payload.get('errorMessage', 'Unknown Lambda error')
                raise PseudonymisationError(f"Lambda function error: {error_msg}")
            
            # Extract pseudonymised NHS number from response
            pseudonymised_nhs = response_payload.get('pseudonymised_nhs_number')
            if not pseudonymised_nhs:
                raise PseudonymisationError("Lambda response missing pseudonymised NHS number")
                
            return pseudonymised_nhs
            
        except (ClientError, BotoCoreError) as e:
            self.logger.error(f"AWS error calling pseudonymisation service: {e}")
            raise PseudonymisationError(f"AWS service error: {str(e)}")
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON response from Lambda: {e}")
            raise PseudonymisationError("Invalid response from pseudonymisation service")
            
        except Exception as e:
            self.logger.error(f"Unexpected error calling pseudonymisation service: {e}")
            raise PseudonymisationError(f"Unexpected error: {str(e)}")

    # Create pseudonymisation service with Lambda function name
    pseudo_service = AWSLambdaPseudonymisationService(
        function_name="barbara-pseudonymisation-function",
        region_name="eu-west-2"  # or whatever region you're using
    )            