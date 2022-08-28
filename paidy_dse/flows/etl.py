
import dask.dataframe as dd
from prefect import task, flow, get_run_logger
from prefect_dask.task_runners import DaskTaskRunner
from dask.dataframe import DataFrame
from datetime import datetime, timedelta
from pytz import utc
from scripts.load import write_parquet_to_s3
from scripts.transformation import clean_golden_data, clean_insight_data
from scripts.common.helpers import *
from scripts.validation import validate_data_in_batch_memory
from typing import Optional, List
import great_expectations as ge
from prefect.orion.schemas.states import Completed, Failed


def load_to(df: DataFrame, location: str, prefix: str, file_name: str) -> bool:
    """ Load data to location e.g: RAW_BUCKET, GOLDEN_BUCKET, ... """
    key = f"{prefix}/{file_name}"

    return write_parquet_to_s3(df, location, key)


def extract_source(file_name) -> Optional[DataFrame]:
    """ Read data from input source """
    try:
        return dd.read_csv(f"{SOURCE_FOLDER}/{file_name}", blocksize="10MB", dtype=RAW_SCHEMA)
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


def validate_stg(ge_context, sub_fol: str, date_str_prefix: str, file_name: str, df: DataFrame) -> bool:
    """ Validate staging data by using Great_expectations """
    res = validate_data_in_batch_memory(ge_context,
                                        df,
                                        date_str_prefix,
                                        sub_fol,
                                        DATA_STAGES[sub_fol]['validation_rules'],
                                        file_name)

    return res


@task
def process(file_name: str, date_str_prefix: str):
    """
    Flow process for each file in source
    Source -> Raw -> Transformation -> Validation -> PRD [Golden | Insight]
    :param file_name: input file name in source
    :param date_str_prefix: run date
    :return:
    """
    logging = get_run_logger()
    logging.info(f"Processing file {file_name}")
    ge_context = ge.get_context()

    # source -> raw
    source = extract_source(file_name)
    # add validation for source here
    is_source_valid = validate_source(source)
    if is_source_valid:
        # raw -> stg
        # load_raw(source, file_name, date_str_prefix)
        load_to(source, RAW_BUCKET, date_str_prefix, file_name)

        # raw = extract_raw(file_name, date_str_prefix)
        stg_golden = clean_golden_data(source)
        stg_insight = clean_insight_data(source)

        # validate
        is_golden_valid = validate_stg(ge_context, "golden", date_str_prefix, file_name, stg_golden)
        is_insight_valid = validate_stg(ge_context, "insight", date_str_prefix, file_name, stg_insight)

        # stg -> prd
        if is_golden_valid:
            # load_golden(extract_stg("golden", date_str_prefix, file_name), date_str_prefix, file_name)
            load_to(stg_golden, GOLDEN_BUCKET, date_str_prefix, file_name)
        if is_insight_valid:
            # load_insight(extract_stg("insight", date_str_prefix, file_name), date_str_prefix, file_name)
            load_to(stg_insight, INSIGHT_BUCKET, date_str_prefix, file_name)

        if is_golden_valid and is_insight_valid:
            return Completed(message=f"File {file_name} ETL done. !")
    return Failed(message=f"File {file_name} ETL failed. !")


@flow(name="Credit ETL Flow", task_runner=DaskTaskRunner)
def etl_flow(file_names: List[str], date_str: str = None):
    """
    ETL:
        - Step 1: Ingest to Raw
        - Step 2: Run transformation then load to Golden & Insight
    :param file_names: list of files in source to be processed
    :param date_str: data identifier, default is "yesterday" in UTC (since we will run on daily basis)
    :return: None
    """
    # get yesterday
    if not date_str:
        # get yesterday
        date_str = datetime.now(utc) - timedelta(days=1)
        date_str = date_str.strftime(DATE_FORMAT)
    date_str_prefix = "/".join(date_str.split("-"))

    for file_name in file_names:
        process.submit(file_name, date_str_prefix)


if __name__ == "__main__":
    files = [f for f in os.listdir(SOURCE_FOLDER) if f.endswith(".csv")]
    etl_flow(files, date_str="2022-08-41")
