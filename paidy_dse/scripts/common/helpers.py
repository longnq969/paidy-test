import os

# source data folder
SOURCE_FOLDER = "./data/sources"

# default chunksize for handling large data files
CHUNKSIZE = 150000

# date format
DATE_FORMAT = "%Y-%m-%d"

RAW_BUCKET = "raw-zone"
GOLDEN_BUCKET = "golden-zone"
INSIGHT_BUCKET = "insight-zone"


DATA_STAGES = {
    "raw": {
        "bucket": "raw-zone",  # s3 bucket
        "file_format": "parquet",
        "validation_rules": None
    },
    "golden": {
        "bucket": "golden-zone",  # s3 bucket
        "file_format": "parquet",
        "validation_rules": "golden_validation"
    },
    "insight": {
        "bucket": "insight-zone",  # s3 bucket
        "file_format": "parquet",
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

RAW_SCHEMA = {"Unnamed: 0": int,
          "SeriousDlqin2yrs": int,
          "RevolvingUtilizationOfUnsecuredLines": float,
          "age": int,
          "NumberOfTime30-59DaysPastDueNotWorse": int,
          "DebtRatio": float,
          "MonthlyIncome": float,
          "NumberOfOpenCreditLinesAndLoans": int,
          "NumberOfTimes90DaysLate": int,
          "NumberRealEstateLoansOrLines": int,
          "NumberOfTime60-89DaysPastDueNotWorse": int,
          "NumberOfDependents": float}



