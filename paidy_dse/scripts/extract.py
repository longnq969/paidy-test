import dask.dataframe as dd
from dask.dataframe import DataFrame
from scripts.common.helpers import S3_CREDENTIAL, CHUNKSIZE
from typing import Optional


def read_parquet_from_s3(bucket_name: str, key: str) -> Optional[DataFrame]:
    """ Read dataframe from s3 """
    try:
        return dd.read_parquet(f's3://{bucket_name}/{key}/*.parquet',
                               chunksize=CHUNKSIZE,
                               storage_options=S3_CREDENTIAL,
                               engine="pyarrow")
    except Exception as e:
        return None

