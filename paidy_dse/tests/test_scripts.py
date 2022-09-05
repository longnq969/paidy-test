from scripts import __version__
from scripts.extract import read_data_from_s3, read_data_from_local
from scripts.transformation import prepare_add_etl_time_column,\
    camelCase_to_snake_case,\
    prepare_drop_columns,\
    prepare_remove_outliers,\
    prepare_remove_duplicates,\
    prepare_fix_inconsistent_values
from scripts.load import write_data_to_s3
import dask.array as da
import dask.dataframe as dd
from scripts.common.helpers import *


def test_version():
    assert __version__ == '0.1.0'


def test_read_data_from_s3():
    df = read_data_from_s3("raw-zone", "/2022/08/24/", "csv").compute()
    assert df.shape[0] == 150000


def test_read_data_from_local():
    df = read_data_from_local("./data/sources", ext="csv").compute()
    assert df.shape[0] == 150000


def test_prepare_add_etl_time_column():
    x = da.ones((4, 2), chunks=(2, 2))
    df = dd.io.from_dask_array(x, columns=['a', 'b'])
    df = prepare_add_etl_time_column(df, "2022-08-21").compute()
    assert 'etl_time' in set(df.columns)
    assert df['etl_time'].unique() == ['2022-08-21']


def test_prepare_rename_columns():
    cols = ['FooBarBaz', 'TestFeatureName50-90Days']
    assert [camelCase_to_snake_case(s) for s in cols] == ['foo_bar_baz', 'test_feature_name50_90_days']


def test_prepare_drop_columns():
    assert





    x = da.ones((4, 2), chunks=(2, 2))
    df = dd.io.from_dask_array(x, columns=['a', 'b'])
    df = prepare_drop_columns(df, ['a']).compute()
    assert df.columns == ['b']


def test_write_s3_data():
    df = dd.read_csv("../data/sources/sample_data.csv").compute()
    write_data_to_s3(df, bucket=DATA_STAGES[STAGING]['bucket'], path="test", ext="parquet")

# def test_prepare_remove_outliers():
#     x = da.ones((4, 2), chunks=(2, 2))
#     df = dd.io.from_dask_array(x, columns=['a', 'b'])
#     df = prepare_drop_columns(df, ['a']).compute()