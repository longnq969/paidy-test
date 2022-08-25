import dask.dataframe as dd
from dask.dataframe import DataFrame
from scripts.common.helpers import S3_CREDENTIAL


def read_data_from_local(fol_path: str, blocksize: str = "10MB", ext="csv") -> DataFrame:
    """
    Read csv folder from path
    :param fol_path: Path to data folder
    :param blocksize: chunksize, to handle large data file
    :param ext: file extension, .csv or .parquet
    :return: DataFrame
    """

    try:
        if ext == "csv":
            return dd.read_csv(f"{fol_path}/*.csv", blocksize=blocksize)
        elif ext == "parquet":
            return dd.read_parquet(f"{fol_path}/*.parquet", blocksize=blocksize)
        else:
            return False
    except Exception as e:
        return False


def read_data_from_s3(bucket: str, path: str, ext: str = "csv") -> DataFrame:
    """
    Read data from S3
    :param bucket: Bucket name
    :param path: file path
    :param ext: file extension, .csv or .parquet
    :return: DataFrame
    """
    try:
        if ext == "csv":
            return dd.read_csv(f's3://{bucket}/{path}/*.csv', storage_options=S3_CREDENTIAL)
        elif ext == "parquet":
            return dd.read_parquet(f's3://{bucket}/{path}/*.parquet', storage_options=S3_CREDENTIAL)
        else:
            return False
        return True
    except Exception as e:
        return False