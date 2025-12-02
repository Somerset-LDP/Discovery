import logging
import os

from aws_lambda_context import LambdaContext

from location.aws_lambda.functions.data_ingestion.imd_data_ingestion import ingest_imd_data
from location.aws_lambda.functions.data_ingestion.onspd_data_ingestion import ingest_onspd_data
from location.aws_lambda.layers.common.common_utils import DataIngestionEvent, DataIngestionSource, \
    DataIngestionException

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("log_level", "DEBUG"))


def handler(event: dict, context: LambdaContext) -> None:
    try:
        logger.debug(f"Received event: {event}")

        required_fields = ["data_source", "s3_bucket", "ingestion-timestamp"]
        missing_fields = [field for field in required_fields if field not in event]
        if missing_fields:
            raise DataIngestionException(f"Missing required event fields: {', '.join(missing_fields)}")

        data_source = event["data_source"]
        valid_sources = [source.value for source in DataIngestionSource]
        if data_source not in valid_sources:
            raise DataIngestionException(
                f"Invalid data_source: '{data_source}'. Valid sources: {', '.join(valid_sources)}")

        ingestion_data = DataIngestionEvent(
            data_source=data_source,
            target_bucket=event["s3_bucket"],
            ingestion_timestamp=event["ingestion-timestamp"],
        )

        if ingestion_data.data_source == DataIngestionSource.ONSPD.value:
            ingest_onspd_data(ingestion_data)

        elif ingestion_data.data_source == DataIngestionSource.IMD_2019.value:
            ingest_imd_data(ingestion_data)

        else:
            msg = f"Unknown data source: {data_source}"
            logger.error(msg)
            raise DataIngestionException(msg)

    except DataIngestionException as e:
        logger.error(f"Data ingestion error: {e.message}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in Lambda handler: {str(e)}", exc_info=True)
        raise DataIngestionException(f"Unexpected error: {str(e)}")
