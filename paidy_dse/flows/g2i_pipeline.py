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
from scripts.common.helpers import RAW_BUCKET, GOLDEN_BUCKET, STG_BUCKET, INSIGHT_BUCKET
from scripts.validation import validate_data_from_s3_data_source
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


@task(name='clean data to ingest to golden zone')
def clean_golden(df: DataFrame) -> DataFrame:
    df = df\
        .pipe(prepare_drop_columns, 'Unnamed: 0')\
        .pipe(prepare_remove_duplicates, [])\
        .pipe(prepare_rename_columns)\
        .pipe(prepare_add_etl_time_column)

    return df


@task(name='clean data to ingest to insight zone')
def clean_insight(df: DataFrame) -> DataFrame:
    df = df\
        .pipe(prepare_drop_columns, 'Unnamed: 0')\
        .pipe(prepare_remove_duplicates, [])\
        .pipe(prepare_fix_inconsistent_values, 'NumberOfTime30-59DaysPastDueNotWorse', [96, 98])\
        .pipe(prepare_fix_inconsistent_values, 'NumberOfTimes90DaysLate', [96, 98])\
        .pipe(prepare_fix_inconsistent_values, 'NumberOfTime60-89DaysPastDueNotWorse', [96, 98])\
        .pipe(prepare_remove_outliers, 'DebtRatio', 2.0)\
        .pipe(prepare_remove_outliers, 'RevolvingUtilizationOfUnsecuredLines', 2.0)\
        .pipe(prepare_remove_outliers, 'MonthlyIncome', 9000.0)\
        .pipe(prepare_rename_columns)\
        .pipe(prepare_add_etl_time_column)

    return df


@task
def validate(date_str: str, checkpoint_name: str, data_stage: str, validation_rules):
    res = validate_data_from_s3_data_source(checkpoint_name, data_stage, validation_rules, date_str)
    return res


@task(name="load data in storage")
def load_data(df: DataFrame, bucket: str = None, path: str = None) -> bool:
    """
    Load data to storage location
    :param df: DataFrame
    :param bucket: Status of data e.g "raw", "golden", "staging", or "insight"
    :param path: path to storage
    :return: None
    """
    # construct path as current date in UTC
    prefixes = datetime.now(utc).strftime("%Y-%m-%d").split('-')
    if path:
        output_path = f"{path}/{prefixes[0]}/{prefixes[1]}/{prefixes[2]}"
    else:
        output_path = f"{prefixes[0]}/{prefixes[1]}/{prefixes[2]}"
    if bucket:
        load_to_s3(df, bucket, output_path)
    else:
        load_to_local(df, output_path)


@flow
def raw_to_golden_flow(df, date_str: str):
    c = clean_golden.submit(df)
    load_data.submit(c, bucket=STG_BUCKET, path="golden")
    validation_res = validate(date_str, "stg_checkpoint", "golden","golden_validation")
    if validation_res:
        load_data(c, bucket=GOLDEN_BUCKET)
        return Completed(message="raw2golden OK")
    return Failed(message="raw2golden Not OK")


@flow
def raw_to_insight_flow(df, date_str: str):
    c = clean_insight.submit(df)
    load_data.submit(c, bucket=STG_BUCKET, path="insight")
    validation_res = validate(date_str, "stg_checkpoint", "insight", "golden_validation")
    if validation_res:
        load_data(c, bucket=INSIGHT_BUCKET)
        return Completed(message="raw2insight OK")
    return Failed(message="raw2insight Not OK")


@flow(task_runner=DaskTaskRunner)
def raw_etl_flow(date_str: str = None):
    if not date_str:
        # get yesterday
        date_str = datetime.now(utc) - timedelta(days=1)
        date_str = date_str.strftime("%Y-%m-%d")
    data = read_raw_data(date_str)
    raw_to_golden_flow(data, date_str)
    raw_to_insight_flow(data, date_str)


if __name__ == "__main__":
    # fol_path = r"data/sources"
    raw_etl_flow(date_str="2022-08-24")