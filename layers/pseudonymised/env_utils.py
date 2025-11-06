import logging
import os
from typing import Dict

logger = logging.getLogger()

REQUIRED_ENV_VARS = [
    'INPUT_S3_BUCKET',
    'INPUT_PREFIX',
    'OUTPUT_S3_BUCKET',
    'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME',
    'KMS_KEY_ID'
]


def get_env_variables() -> Dict[str, str]:
    env_vars = {var: os.getenv(var, '').strip() for var in REQUIRED_ENV_VARS}

    missing = [var for var, val in env_vars.items() if not val]
    if missing:
        error_msg = f"Missing required environment variables: {', '.join(missing)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    return env_vars
