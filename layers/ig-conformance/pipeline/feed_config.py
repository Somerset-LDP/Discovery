from dataclasses import dataclass


@dataclass(frozen=True)
class FeedConfig:
    feed_type: str
    nhs_column_index: int
    metadata_rows_to_skip: int
    preserve_metadata: bool


GP_FEED = FeedConfig(
    feed_type="gp",
    nhs_column_index=0,
    metadata_rows_to_skip=2,
    preserve_metadata=True
)

SFT_FEED = FeedConfig(
    feed_type="sft",
    nhs_column_index=1,
    metadata_rows_to_skip=0,
    preserve_metadata=False
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

