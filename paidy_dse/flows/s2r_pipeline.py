from prefect import task, flow, get_run_logger
from prefect_dask.task_runners import DaskTaskRunner
import os
import dask.dataframe as dd
from dask.dataframe import DataFrame
from datetime import datetime
from pytz import utc
from scripts.load import load_to_local, load_to_s3
from scripts.extract import read_data_from_local
from scripts.common.helpers import RAW_BUCKET
from prefect.orion.schemas.states import Completed, Failed


@task(name='read source data')
def extract(path: str, blocksize="10MB") -> DataFrame:
    """
    Read csv folder from path
    :param path: Path to data folder
    :return: DataFrame
    """
    return read_data_from_local(path, blocksize=blocksize)


@task(name="load data in storage")
def load(df: DataFrame, bucket: str = RAW_BUCKET, path: str = None) -> bool:
    """
    Load data to storage location
    :param df: DataFrame
    :param bucket: Status of data e.g "raw", "golden", "staging", or "insight"
    :param path: path to storage
    :return: None
    """
    if not path:
        # construct path as current date in UTC
        path = "/".join(datetime.now(utc).strftime("%Y-%m-%d").split('-'))
    return load_to_s3(df, bucket, path, ext="csv")


@flow(task_runner=DaskTaskRunner)
def source_to_raw_flow(path):
    data = extract(path)
    # check data size
    if data.shape[0].compute() > 0:
        # load to raw
        res = load(data, bucket='raw-zone')
        if res:
            return Completed(message="Loaded completed.!")
        return Failed(message="Load failed.!")


if __name__ == "__main__":
    fol_path = r"data/sources"
    source_to_raw_flow(fol_path)