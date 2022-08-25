import os

# S3 buckets
RAW_BUCKET = "raw-zone"
GOLDEN_BUCKET = "golden-zone"
STAGING_BUCKET = "staging-zone"
INSIGHT_BUCKET = "insight-zone"

# date format
DATE_FORMAT = "%Y-%m-%d"

# prefix
INSIGHT_PREFIX = "insight"
GOLDEN_PREFIX = "golden"

# data stages
RAW = "raw"
GOLDEN = "golden"
INSIGHT = "insight"
STAGING = "staging"


DATA_STAGES = {
    "raw": {
        "bucket": RAW_BUCKET,
        "file_format": "csv"
    },
    "golden": {
        "bucket": GOLDEN_BUCKET,
        "file_format": "csv"
    },
    "staging": {
        "bucket": STAGING_BUCKET,
        "file_format": "parquet"
    },
    "insight": {
        "bucket": INSIGHT_BUCKET,
        "file_format": "csv"
    }
}

# GE suite name
GOLDEN_EXP_SUITE_NAME = "golden_validation"
INSIGHT_EXP_SUITE_NAME = "golden_validation"

# S3 credential
S3_CREDENTIAL = {
    'key': os.environ['AWS_ACCESS_KEY_ID'],
    'secret': os.environ['AWS_SECRET_ACCESS_KEY'],
    'client_kwargs': {
        'endpoint_url': os.environ['AWS_ENDPOINT_URL']
    },
    'use_ssl': False
}