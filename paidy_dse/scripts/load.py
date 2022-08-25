from dask.dataframe import DataFrame
import os
from scripts.common.helpers import S3_CREDENTIAL


def load_to_local(df: DataFrame, path: str, ext="csv") -> bool:
    try:
        if ext == "csv":
            df.to_csv(f"{path}/data.csv")
        elif ext == "parquet":
            df.to_parquet(f"{path}/data.parquet")
        else:
            return False
        return True
    except IOError as e:
        # handle exception folder not existed
        # print(e)
        return False


def load_to_s3(df: DataFrame, bucket: str, path: str, ext: str = "csv") -> bool:
    try:
        if ext == "csv":
            df.to_csv(f's3://{bucket}/{path}/data-*.csv', storage_options=S3_CREDENTIAL, mode='w')
        elif ext == "parquet":
            df.to_parquet(f's3://{bucket}/{path}/data-*.parquet', storage_options=S3_CREDENTIAL, mode='w')
        else:
            return False
        return True
    except Exception as e:
        # print(e)
        return False
