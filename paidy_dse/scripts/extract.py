import dask.dataframe as dd
from dask.dataframe import DataFrame
from scripts.common.helpers import S3_CREDENTIAL
from typing import Optional


def read_data_from_local(fpath: str, blocksize: str = "10MB", ext="csv") -> Optional[DataFrame]:
    """
    Read csv folder from path
    :param fpath: Path to data folder
    :param blocksize: chunksize, to handle large data file and add parallelism
    :param ext: file extension, .csv or .parquet
    :return: DataFrame or None
    """
    if ext == "csv":
        return dd.read_csv(f"{fpath}/*.csv", blocksize=blocksize)
    elif ext == "parquet":
        return dd.read_parquet(f"{fpath}/*.parquet", chunksize=150000)
    else:
        return None


def read_data_from_s3(bucket: str, path: str, ext: str = "csv") -> Optional[DataFrame]:
    """
    Read data from S3
    :param bucket: Bucket name
    :param path: file path
    :param ext: file extension, .csv or .parquet
    :return: DataFrame or None
    """
    if ext == "csv":
        return dd.read_csv(f's3://{bucket}/{path}/*.csv', storage_options=S3_CREDENTIAL)
    elif ext == "parquet":
        return dd.read_parquet(f's3://{bucket}/{path}/*.parquet', storage_options=S3_CREDENTIAL)
    else:
        return None

