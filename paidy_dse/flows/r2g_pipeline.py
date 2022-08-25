from prefect import task, flow, get_run_logger
from prefect_dask.task_runners import DaskTaskRunner
import os
import dask.dataframe as dd
from dask.dataframe import DataFrame
from datetime import datetime, timedelta
from pytz import utc
from scripts.load import load_to_local, load_to_s3
from scripts.transformation import prepare_add_etl_time_column,\
    prepare_drop_columns,\
    prepare_remove_duplicates,\
    prepare_remove_outliers,\
    prepare_fix_inconsistent_values,\
    prepare_rename_columns
from scripts.extract import read_data_from_local, read_data_from_s3
from scripts.common.helpers import RAW_BUCKET, GOLDEN_BUCKET, STG_BUCKET
from prefect.orion.schemas.states import Completed, Failed


@task(name='read raw data')
def read_raw_data(date_str: str = None) -> DataFrame:
    if date_str is None:
        # get yesterday
        date_str = datetime.now(utc) - timedelta(1)
        date_str = date_str.strftime("%Y-%m-%d")
    key = "/".join(date_str.split("-"))
    logger = get_run_logger()
    logger.info(f"Reading data from bucket: {RAW_BUCKET}, key: {key}")

    return read_data_from_s3(RAW_BUCKET, key)


@task(name='clean data')
def clean(df: DataFrame) -> DataFrame:
    df = df\
        .pipe(prepare_drop_columns, 'Unnamed: 0')\
        .pipe(prepare_remove_duplicates, [])\
        .pipe(prepare_rename_columns)\
        .pipe(prepare_add_etl_time_column)

    return df


@task
def validate(date_str: str, checkpoint_name: str):
    return True


@task(name="load data in storage")
def load_data(df: DataFrame, bucket: str = None, path: str = './') -> bool:
    """
    Load data to storage location
    :param df: DataFrame
    :param bucket: Status of data e.g "raw", "golden", "staging", or "insight"
    :param path: path to storage
    :return: None
    """
    # construct path as current date in UTC
    prefixes = datetime.now(utc).strftime("%Y-%m-%d").split('-')
    if bucket:
        output_path = f"{prefixes[0]}/{prefixes[1]}/{prefixes[2]}"
        load_to_s3(df, bucket, output_path)
    else:
        output_path = f"{path}/{prefixes[0]}/{prefixes[1]}/{prefixes[2]}"
        load_to_local(df, output_path)


@flow(task_runner=DaskTaskRunner)
def raw_to_golden_flow(date_str: str = None):
    if not date_str:
        # get yesterday
        date_str = datetime.now(utc) - timedelta(days=1)
        date_str = date_str.strftime("%Y-%m-%d")
    data = read_raw_data(date_str)
    # cleaning data
    cleaned_data = clean(data)

    # load to Staging zone
    load_data(cleaned_data, bucket=STG_BUCKET)

    # run GE validation
    validation_res = validate(date_str, checkpoint_name="raw2golden-validation")
    if not validation_res:
        return Failed(message="Data [raw -> golden] validation failed.!")

    # load to Golden zone
    load_data(cleaned_data, bucket=GOLDEN_BUCKET)

    return Completed(message="Data [raw -> golder] ingestion completed.!")


if __name__ == "__main__":
    # fol_path = r"data/sources"
    raw_to_golden_flow(date_str="2022-08-24")