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


def camelCase_to_snake_case(s: str) -> str:
    """ Change format of string from camelCase to snake_case """
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


def prepare_loans_columns(df: DataFrame) -> DataFrame:
    """
    Coalesce column "NumberOfOpenCreditLinesAndLoans" and "NumberRealEstateLoansOrLines"
    NumberRealEstateLoansOrLines >= NumberRealEstateLoansOrLines
    :param df: DataFrame
    :return: DataFrame
    """
    df['NumberOfOpenCreditLinesAndLoans'] = df['NumberOfOpenCreditLinesAndLoans'].\
        combine_first(df['NumberRealEstateLoansOrLines'])

    return df

def clean_golden_data(df: DataFrame) -> DataFrame:
    """ Data transformation steps for Golden-zone """
    def process(df_batch):
        df_batch = df_batch\
            .pipe(prepare_drop_columns, ["Unnamed: 0"])\
            .pipe(prepare_remove_duplicates, [])\
            .pipe(prepare_rename_columns)\
            .pipe(prepare_add_etl_time_column)

        return df_batch
    df = df.map_partitions(process)
    return df


def clean_insight_data(df: DataFrame) -> DataFrame:
    """ Data transformation steps for Insight-zone  """
    def process(df_batch):
        df_batch = df_batch \
            .pipe(prepare_drop_columns, ["Unnamed: 0"])\
            .pipe(prepare_remove_duplicates, [])\
            .pipe(prepare_fix_inconsistent_values, 'NumberOfTime30-59DaysPastDueNotWorse', [96, 98])\
            .pipe(prepare_fix_inconsistent_values, 'NumberOfTimes90DaysLate', [96, 98])\
            .pipe(prepare_fix_inconsistent_values, 'NumberOfTime60-89DaysPastDueNotWorse', [96, 98]) \
            .pipe(prepare_remove_duplicates, []) \
            .pipe(prepare_remove_outliers, 'DebtRatio', 2.0)\
            .pipe(prepare_remove_outliers, 'RevolvingUtilizationOfUnsecuredLines', 2.0)\
            .pipe(prepare_remove_outliers, 'MonthlyIncome', 9000.0)\
            .pipe(prepare_rename_columns)\
            .pipe(prepare_add_etl_time_column)
        # df = df.repartition(partition_size="10MB")
        return df_batch
    df = df.map_partitions(process)
    return df
