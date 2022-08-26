import logging

from prefect import task, flow, get_run_logger
from prefect_dask.task_runners import DaskTaskRunner
from dask.dataframe import DataFrame
from datetime import datetime, timedelta
from pytz import utc
from scripts.load import write_data_to_s3
from scripts.transformation import clean_golden_data, clean_insight_data
from scripts.extract import read_data_from_s3, read_data_from_local
from scripts.common.helpers import *
from scripts.validation import validate_data_from_s3_data_source
from prefect.orion.schemas.states import Completed, Failed
from typing import Optional


@task(name="Read data from S3 source")
def extract_s3_data(source: str, date_str: str, prefix: str = None) -> Optional[DataFrame]:
    """
    Extract data from S3
    :param source: datasource id, e.g "raw", "golden", "staging", or "insight"
    :param date_str: date string in format "%Y-%m-%d"
    :param prefix: prefix in case of reading from staging-zone
    :return: DataFrame
    """
    if not date_str:
        # get yesterday
        date_str = datetime.now(utc).strftime(DATE_FORMAT)
    fpath = "/".join(date_str.split('-'))
    bucket, file_format, _ = list(DATA_STAGES[source].values())
    if source == STAGING:
        fpath = f"{prefix}/{fpath}"

    # extract data
    return read_data_from_s3(bucket, fpath, file_format)


@task(name='Extract data from Local source')
def extract_local_data(fpath: str, blocksize="10MB") -> Optional[DataFrame]:
    """ Read data from folder path """

    return read_data_from_local(fpath, blocksize=blocksize)


@task(name="Validate data in S3")
def validate_s3_data(date_str: str, checkpoint_name: str, data_stage: str, validation_rules):
    """
    Use great_expectations to validate data before ingesting
    """
    logging = get_run_logger()
    res = validate_data_from_s3_data_source(checkpoint_name, data_stage, validation_rules, date_str)

    if res['success']:
        logging.info("PASSED data validation .!")
    else: logging.info("FAILED data validation .!")
    return res['success']


@task(name="Write data to S3")
def load_s3_data(df: DataFrame, dest: str, date_str: str = None, prefix: str = None, is_valid: bool = True) -> bool:
    """
    Load data to storage location
    :param df: DataFrame
    :param dest: Location to write e.g "raw", "golden", "staging", or "insight"
    :param date_str: date string in format "%Y-%m-%d"
    :param prefix: prefix in case of writing to staging
    :param is_valid: great_expectation validation result
    :return: bool
    """
    if not is_valid:
        return False
    if not date_str:
        # get yesterday
        date_str = datetime.now(utc).strftime(DATE_FORMAT)
    fpath = "/".join(date_str.split('-'))
    bucket, file_format, _ = list(DATA_STAGES[dest].values())
    if dest == STAGING:
        fpath = f"{prefix}/{fpath}"

    # load data
    return write_data_to_s3(df, bucket, fpath, file_format)


@flow(name="Load source data in Raw-zone")
def extract_source_then_load_to_raw(fpath, date_str: str = None):
    data = extract_local_data(fpath)
    # check data size then load to raw-zone
    if len(data.index) > 0:
        # check columns
        if len(data.columns) == len(RAW_COLUMNS):
            if set(data.columns) == set(RAW_COLUMNS):
                return load_s3_data(data, RAW, date_str)
    return False


@flow(
    name="Transformation and store in Staging-zone",
    task_runner=DaskTaskRunner(
        cluster_kwargs={"n_workers": 2}
))
def transform_then_load_to_staging(df, date_str: str):
    load_s3_data.submit(clean_golden_data(df), STAGING, date_str, GOLDEN_PREFIX)
    load_s3_data.submit(clean_insight_data(df), STAGING, date_str, INSIGHT_PREFIX)


@flow(
    name="Load to Golden and Insight zone",
    task_runner=DaskTaskRunner(
        cluster_kwargs={"n_workers": 2}
))
def validate_from_staging_then_load(date_str: str):
    load_s3_data.submit(extract_s3_data(STAGING, date_str, GOLDEN_PREFIX),
                        GOLDEN, date_str, None,
                        is_valid=validate_s3_data(date_str, "stg_checkpoint", GOLDEN,
                                                  DATA_STAGES[GOLDEN]["validation_rules"]))
    load_s3_data.submit(extract_s3_data(STAGING, date_str, INSIGHT_PREFIX),
                        INSIGHT, date_str, None,
                        is_valid=validate_s3_data(date_str, "stg_checkpoint", INSIGHT,
                                                  DATA_STAGES[INSIGHT]["validation_rules"]))


@flow(name="Credit ETL Flow")
def etl_flow(fpath: str, date_str: str = None):
    """
    ETL:
        - Step 1: Ingest to Raw
        - Step 2: Run transformation from Raw to ingest to Golden & Insight
    :param fpath: source folder path
    :param date_str: data identifier, default is "yesterday" in UTC (since we will run on daily basis)
    :return: None
    """
    if not date_str:
        # get yesterday
        date_str = datetime.now(utc) - timedelta(days=1)
        date_str = date_str.strftime(DATE_FORMAT)
    # load to raw-zone
    extract_source_then_load_to_raw(fpath, date_str)
    # extract from raw-zone for further transformations
    data = extract_s3_data(RAW, date_str, return_state=True)
    if data.result() is not None:
        # load to staging-zone
        transform_then_load_to_staging(data, date_str)
        # load to golden-zone & insight-zone
        validate_from_staging_then_load(date_str)


if __name__ == "__main__":
    fol_path = r"data/sources"
    etl_flow(fol_path, date_str="2022-08-25")