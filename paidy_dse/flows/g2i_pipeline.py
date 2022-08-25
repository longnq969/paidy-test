from prefect import task, flow, get_run_logger
from prefect_dask.task_runners import DaskTaskRunner
import os
import dask.dataframe as dd
from typing import Optional
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
def extract_from_raw(date_str: str = None) -> DataFrame:
    """
    Extract data from Raw bucket
    :param date_str: Date identity, string in format "%Y-%m-%d"
    :return: DataFrame
    """
    if date_str is None:
        # get yesterday
        date_str = datetime.now(utc) - timedelta(1)
        date_str = date_str.strftime("%Y-%m-%d")
    fpath = "/".join(date_str.split("-"))
    logger = get_run_logger()
    logger.info(f"Reading data from bucket: {RAW_BUCKET}, fpath: {fpath}")

    return read_data_from_s3(RAW_BUCKET, fpath, ext="csv")


# @task(name='clean data to ingest to golden zone')
def clean_golden_data(df: DataFrame) -> DataFrame:
    """ Data cleaning before ingesting into Golden bucket """
    df = df\
        .pipe(prepare_drop_columns, 'Unnamed: 0')\
        .pipe(prepare_remove_duplicates, [])\
        .pipe(prepare_rename_columns)\
        .pipe(prepare_add_etl_time_column)

    return df


# @task(name='clean data to ingest to insight zone')
def clean_insight_data(df: DataFrame) -> DataFrame:
    """ Data cleaning before ingesting into Insight bucket """
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
def clean_data(df: DataFrame, dest: str) -> Optional[DataFrame]:
    if dest == "golden":
        return clean_golden_data(df)
    elif dest == "insight":
        return clean_insight_data(df)
    else:
        return None


@task(name="validate data before Ingesting")
def validate_data(date_str: str, checkpoint_name: str, data_stage: str, validation_rules):
    """
    Use great_expectations to validate data before ingesting
    :param date_str: date string in format "%Y-%m-%d", identify data path
    :param checkpoint_name: GE config - checkpoint_name
    :param data_stage: GE config - data_asset_name
    :param validation_rules: GE config - expectation_suit_name
    :return: bool
    """
    return validate_data_from_s3_data_source(checkpoint_name, data_stage, validation_rules, date_str)


@task(name="load data in storage")
def load_data(df: DataFrame, dest: str, prefix: str = None) -> bool:
    """
    Load data to storage location
    :param df: DataFrame
    :param dest: Location to write e.g "raw", "golden", "staging", or "insight"
    :param prefix: path prefix
    :return: None
    """
    # construct path as current date in UTC
    fpath = "/".join(datetime.now(utc).strftime("%Y-%m-%d").split('-'))
    if prefix:
        fpath = f"{prefix}/{fpath}"

    if dest == "golden":
        load_to_s3(df, GOLDEN_BUCKET, fpath, "csv")
    elif dest == "staging":
        load_to_s3(df, STG_BUCKET, fpath, "parquet")
    elif dest == "insight":
        load_to_s3(df, INSIGHT_BUCKET, fpath, "csv")
    else:
        return False


@flow
def raw_to_destination_flow(df, date_str: str, dest: str):
    res = False
    logging = get_run_logger()
    # clean data
    c = clean_data.submit(df, dest)
    # load to staging zone
    load_data.submit(c, "staging", dest)
    # validate data
    is_data_valid = validate_data.submit(date_str, "stg_checkpoint", dest, f"{dest}_validation")
    if is_data_valid.result():
        logging.info(f"raw2{dest} validation passed .!")
        l = load_data.submit(c, dest)
            # logging.info(f"raw2{dest} completed .!")
            # return Completed(message=f"raw2{dest} completed .!")
        if l.result():
            return True
        # Failed(message=f"raw2{dest} validation failed .!")
        return False
    # return Failed(message=f"raw2{dest} validation failed .!")
    return False


@flow(task_runner=DaskTaskRunner)
def raw_etl_flow(date_str: str = None):
    if not date_str:
        # get yesterday
        date_str = datetime.now(utc) - timedelta(days=1)
        date_str = date_str.strftime("%Y-%m-%d")
    data = extract_from_raw(date_str)
    r2g_result = raw_to_destination_flow(data, date_str, 'golden')
    r2i_result = raw_to_destination_flow(data, date_str, 'insight')

    if r2i_result and r2g_result:
        return Completed(message="ETL completed .!")
    else:
        return Failed(message="ETL failed .!")


if __name__ == "__main__":
    # fol_path = r"data/sources"
    raw_etl_flow(date_str="2022-08-24")