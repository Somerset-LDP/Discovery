from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class FeedConfig:
    feed_type: str
    metadata_rows_to_skip: int
    db_columns: Dict[str, int]  # DB column name -> CSV column position
    csv_auxiliary_columns: Dict[str, int]
    validation_rules: Dict[str, Any]


GP_FEED = FeedConfig(
    feed_type="gp",
    metadata_rows_to_skip=2,
    db_columns={
        'nhs_number': 0,
        'given_name': 1,
        'family_name': 2,
        'date_of_birth': 3,
        'postcode': 4,
        'sex': 6,
        'height_cm': 7,
        'height_observation_time': 9,
        'weight_kg': 10,
        'weight_observation_time': 12,
    },
    csv_auxiliary_columns={
        'first_line_of_address': 5,
        'height_unit': 8,
        'weight_unit': 11,
        'consultation_id': 13,
        'consultation_date': 14,
        'consultation_time': 15,
        'consultation_type': 16,
        'user_type': 17,
    },
    validation_rules={
        'required_patient_fields': ['nhs_number', 'given_name', 'family_name', 'date_of_birth', 'postcode', 'sex'],
        'valid_date_format': '%d-%b-%y',
        'has_measurements': True,
        'height_unit': 'cm',
        'weight_unit': 'kg',
        'allow_empty_measurements': True
    }
)

SFT_FEED = FeedConfig(
    feed_type="sft",
    metadata_rows_to_skip=0,
    db_columns={
        'nhs_number': 1,
        'given_name': 2,
        'family_name': 3,
        'date_of_birth': 4,
        'sex': 5,
        'postcode': 6,
    },
    csv_auxiliary_columns={
        'pas_number': 0,
        'first_line_of_address': 7,
    },
    validation_rules={
        'required_patient_fields': ['nhs_number', 'given_name', 'family_name', 'date_of_birth', 'sex', 'postcode'],
        'valid_date_format': '%Y-%m-%d',
        'has_measurements': False
    }
)

FEED_CONFIGS = {
    "gp": GP_FEED,
    "sft": SFT_FEED
}


def get_feed_config(feed_type: str) -> FeedConfig:
    config = FEED_CONFIGS.get(feed_type.lower())
    if not config:
        raise ValueError(f"Unsupported feed_type: {feed_type}. Must be one of: {', '.join(FEED_CONFIGS.keys())}")
    return config
