import os

RAW_BUCKET = "raw-zone"
GOLDEN_BUCKET = "golden-zone"
STG_BUCKET = "staging-zone"
INSIGHT_BUCKET = "insight-zone"
DATE_FORMAT = "%Y-%m-%d"

S3_CREDENTIAL = {
    'key': os.environ['AWS_ACCESS_KEY_ID'],
    'secret': os.environ['AWS_SECRET_ACCESS_KEY'],
    'client_kwargs': {
        'endpoint_url': os.environ['AWS_ENDPOINT_URL']
    },
    'use_ssl': False
}