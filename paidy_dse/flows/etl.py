
import dask.dataframe as dd
from dask.dataframe import DataFrame
import great_expectations as ge
from scripts.load import write_parquet_to_s3
from scripts.transformation import clean_golden_data, clean_insight_data
from scripts.common.helpers import *
from scripts.validation import validate_data_in_batch_memory
from prefect import task, flow, get_run_logger
from prefect_dask.task_runners import DaskTaskRunner
from prefect.orion.schemas.states import Completed, Failed
from prefect.tasks import task_input_hash
from typing import Optional, List
from pytz import utc
from datetime import datetime, timedelta
import gc

def load_to(df: DataFrame, location: str, prefix: str, file_name: str, sub_fol: str = None) -> bool:
    """ Load data to location e.g: RAW_BUCKET, GOLDEN_BUCKET, ... """
    if sub_fol:
        key = f"{sub_fol}/{prefix}/{file_name}"
    else:
        key = f"{prefix}/{file_name}"

    return write_parquet_to_s3(df, location, key)


def extract_source(file_path) -> Optional[DataFrame]:
    """ Read data from input source """
    try:
        return dd.read_csv(file_path, blocksize=None, dtype=RAW_SCHEMA)
    except Exception as e:
        return None


def validate_source(df: DataFrame):
    """ Validate data input from source """
    if df is not None:
        if len(df.index) > 0:
            # check columns
            if len(df.columns) == len(RAW_SCHEMA.keys()):
                # check column list
                if set(df.columns) == set(RAW_SCHEMA.keys()):
                    return True

    return False


def validate_stg(ge_context, stage: str, date_str_prefix: str, file_name: str, df: DataFrame) -> bool:
    """ Validate staging data by using Great_expectations """
    res = validate_data_in_batch_memory(ge_context,
                                        df,
                                        date_str_prefix,
                                        stage,
                                        DATA_STAGES[stage]['validation_rules'],
                                        file_name)

    return res


# @task(retries=3, retry_delay_seconds=60, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
@task()
def process(file_path: str, date_str_prefix: str):
    """
    Flow process for each file in source
    Source -> Raw -> Transformation -> Validation -> PRD [Golden | Insight]
    :param file_path: input file path
    :param date_str_prefix: run date
    :return:
    """
    logging = get_run_logger()
    logging.info(f"Processing file {file_path}")

    # get file_name from path
    file_name = file_path.split("/")[-1]

    # get great_expectation context
    ge_context = ge.get_context()

    # source -> raw
    source = extract_source(file_path)

    # persist
    source = source.persist()

    # add validation for source here
    is_source_valid = validate_source(source)
    if is_source_valid:
        # raw -> stg
        # load_raw(source, file_name, date_str_prefix)
        load_to(source, RAW_BUCKET, date_str_prefix, file_name)

        # raw = extract_raw(file_name, date_str_prefix)
        stg_golden = clean_golden_data(source)
        stg_insight = clean_insight_data(source)

        # persist
        stg_golden = stg_golden.persist()
        stg_insight = stg_insight.persist()

        # transform -> stg
        load_to(stg_golden, STAGING_BUCKET, date_str_prefix, file_name, sub_fol="golden")
        load_to(stg_insight, STAGING_BUCKET, date_str_prefix, file_name, sub_fol="insight")

        # validate
        is_golden_valid = validate_stg(ge_context, "golden", date_str_prefix, file_name, stg_golden)
        is_insight_valid = validate_stg(ge_context, "insight", date_str_prefix, file_name, stg_insight)
        # is_insight_valid = True

        # stg -> prd
        if is_golden_valid:
            # load_golden(extract_stg("golden", date_str_prefix, file_name), date_str_prefix, file_name)
            load_to(stg_golden, GOLDEN_BUCKET, date_str_prefix, file_name)
        if is_insight_valid:
            # load_insight(extract_stg("insight", date_str_prefix, file_name), date_str_prefix, file_name)
            load_to(stg_insight, INSIGHT_BUCKET, date_str_prefix, file_name)

        # delete data & gc collect
        del source
        del stg_golden
        del stg_insight
        gc.collect()

        if is_golden_valid and is_insight_valid:
            return Completed(message=f"File {file_name} ETL done. !")
    return Failed(message=f"File {file_name} ETL failed. !")


@flow(name="Credit ETL Flow", task_runner=DaskTaskRunner)
def etl_flow(source_folder: str, date_str: str = None):
    """
    ETL:
        - Step 1: Ingest to Raw
        - Step 2: Run transformation then load to Golden & Insight
    :param source_folder: source folder
    :param date_str: data identifier, default is "yesterday" in UTC (since we will run on daily basis)
    :return: None
    """
    # get yesterday
    if not date_str:
        # get yesterday
        date_str = datetime.now(utc) - timedelta(days=1)
        date_str = date_str.strftime(DATE_FORMAT)
    date_str_prefix = "/".join(date_str.split("-"))

    # get file path in source folder
    file_paths = [f"{source_folder}/{f}" for f in os.listdir(source_folder) if f.endswith(".csv")]
    
    for file_path in file_paths:
        for i in range(10):
            process.submit(file_path, date_str_prefix)


if __name__ == "__main__":
    # files = [f for f in os.listdir(SOURCE_FOLDER) if f.endswith(".csv")]
    source_folder = "./data/sources"
    etl_flow(source_folder, date_str="2022-08-41")
