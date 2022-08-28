from dask.dataframe import DataFrame
from scripts.common.helpers import S3_CREDENTIAL

def write_parquet_to_s3(df: DataFrame, bucket_name: str, key: str) -> bool:
    """ Write dataframe to S3"""
    try:
        df.to_parquet(f"s3://{bucket_name}/{key}/",
                      storage_options=S3_CREDENTIAL,
                      engine="pyarrow",
                      write_index=True)
        return True
    except Exception as e:  # any exception
        return False



