from dataclasses import dataclass


@dataclass(frozen=True)
class FeedConfig:
    feed_type: str
    metadata_rows_to_skip: int
    preserve_metadata: bool
    fields_to_pseudonymise: dict
    validation_rules: dict


GP_FEED = FeedConfig(
    feed_type="gp",
    metadata_rows_to_skip=2,
    preserve_metadata=True,
    fields_to_pseudonymise= {
        'NHS Number': 'nhs_number',
        'Given Name': 'given_name',
        'Family Name': 'family_name',
        'Date of Birth': 'date_of_birth',
        'Gender': 'gender',
        'Postcode': 'postcode'
    },
    validation_rules={
        "valid_sex_values": ['Male', 'Female', 'Indeterminate'],
        "valid_date_format": "%d-%b-%y"
    }

)

SFT_FEED = FeedConfig(
    feed_type="sft",
    metadata_rows_to_skip=0,
    preserve_metadata=False,
    fields_to_pseudonymise={
        'nhs_number': 'nhs_number',
        'first_name': 'first_name',
        'last_name': 'last_name',
        'date_of_birth': 'date_of_birth',
        'sex': 'sex',
        'postcode': 'postcode'
    },
    validation_rules={
        "valid_sex_values": ['1', '2', '9'],
        "valid_date_format": "%Y-%m-%d"
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

