import os

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
        "bucket": "raw-zone",  # s3 bucket
        "file_format": "csv",
        "validation_rules": None
    },
    "golden": {
        "bucket": "golden-zone",  # s3 bucket
        "file_format": "csv",
        "validation_rules": "golden_validation"
    },
    "staging": {
        "bucket": "staging-zone",  # s3 bucket
        "file_format": "parquet",
        "validation_rules": None
    },
    "insight": {
        "bucket": "insight-zone",  # s3 bucket
        "file_format": "csv",
        "validation_rules": "insight_validation"
    }
}

# S3 credential
S3_CREDENTIAL = {
    'key': os.environ['AWS_ACCESS_KEY_ID'],
    'secret': os.environ['AWS_SECRET_ACCESS_KEY'],
    'client_kwargs': {
        'endpoint_url': os.environ['AWS_ENDPOINT_URL']
    },
    'use_ssl': False
}