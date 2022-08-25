from dask.dataframe import DataFrame
import os
from scripts.common.helpers import S3_CREDENTIAL


def write_data_to_s3(df: DataFrame, bucket: str, path: str, ext: str = "csv") -> bool:
    if ext == "csv":
        df.to_csv(f's3://{bucket}/{path}/data-*.csv', storage_options=S3_CREDENTIAL)
        return True
    elif ext == "parquet":
        df.to_parquet(f's3://{bucket}/{path}/', storage_options=S3_CREDENTIAL)
        return True
    else:
        return False
