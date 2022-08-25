from dask.dataframe import DataFrame
import dask.dataframe as dd
from datetime import datetime
from pytz import utc
import re


def prepare_add_etl_time_column(df: DataFrame, datetime_value: str = None) -> DataFrame:
    """
    Add ETL datetime column to data ingested
    :param df: Dataframe
    :param datetime_value: datetime value to be added
    :return: DataFrame
    """
    now = datetime.now(tz=utc)
    dt_string = datetime_value or now.strftime("%Y-%m-%d")
    df['etl_time'] = dt_string

    return df


def prepare_drop_columns(df: DataFrame, to_drop: list=[]) -> DataFrame:
    """
    Removing columns from data
    :param df: DataFrame
    :param to_drop: list of columns []
    :return: DataFrame
    """
    if len(to_drop) > 0:
        df = df.drop(to_drop, axis=1)

    return df


def prepare_remove_duplicates(df: DataFrame, subset: list = []) -> DataFrame:
    """
    Removing duplicated rows from data
    :param df: DataFrame
    :param subset: list of columns to be checked
    :return: DataFrame
    """
    if len(subset) > 0:
        df = df.drop_duplicates(subset=subset, keep='first')
    else:
        df = df.drop_duplicates(keep='first')

    return df


def prepare_remove_outliers(df: DataFrame, col: str, threshold: int) -> DataFrame:
    """
    Removing outliers from data
    :param df: DataFrame
    :param col: column name
    :param threshold: threshold value
    :return: DataFrame
    """
    df[col] = df[col].clip(upper=threshold)

    return df


def prepare_fix_inconsistent_values(df: DataFrame, col: str, values: list) -> DataFrame:
    """
    Fix inconsistent values in data
    :param df: DataFrame
    :param col: column name
    :param values: list of values to be replaced
    :return: DataFrame
    """
    df[col] = df[col].replace(values, None)

    return df


def camelCase_to_snake_case(s):
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    return pattern.sub('_', s.replace('-', '_')).lower()


def prepare_rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename columns, from camelCase to snake_case
    :param df: DataFrame
    :return: DataFrame
    """
    cols_dict = {c: camelCase_to_snake_case(c) for c in df.columns}
    df = df.rename(columns=cols_dict)

    return df
