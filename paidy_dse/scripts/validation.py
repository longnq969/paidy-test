import great_expectations as ge
from great_expectations.core.batch import BatchRequest
import datetime
from pandas import DataFrame
from scripts.common.helpers import DATE_FORMAT


def validate_data_from_s3_data_source(checkpoint_name: str,
                                      data_asset_name: str,
                                      ge_suite_name: str,
                                      date_str: str = None):
    """
    Validate data using great_expectation checkpoint defined in yaml file.
    :param checkpoint_name: GE config - checkpoint_name
    :param data_asset_name: GE config - data_asset_name
    :param ge_suite_name: GE config - expectation_suit_name
    :param date_str: date string in format "%Y-%m-%d", identify data path
    :return: validation result
    """
    if not date_str:
        date_str = datetime.now().strftime(DATE_FORMAT)
    year, month, day = date_str.split("-")

    # retrieve great_expectations context
    context = ge.get_context()

    # init batch request
    batch_request = BatchRequest(
        datasource_name="stg_datasource",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name=data_asset_name,
        data_connector_query={
            "batch_filter_parameters": {
                "year": year,
                "month": month,
                "day": day
            },
            # "index": [1, 90],
            "limit": 90
        }
    )

    results = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        batch_request=batch_request,
        expectation_suite_name=ge_suite_name,
        run_name_template=f"{data_asset_name}_{date_str}_%Y%m%d",
    )

    return results


def validate_data_in_batch_memory(ge_context,
                                  df: DataFrame,
                                  date_str_prefix: str,
                                  data_asset_name: str,
                                  ge_suite_name: str,
                                  file_name: str):
    """
    Run validation against in memory dataset
    :param ge_context: GE context from great_expectations.yml
    :param df: DataFrame
    :param date_str_prefix: date string to identify run name
    :param data_asset_name: data stage. e.g: "golden" or "insight"
    :param ge_suite_name: GE expectation suite name
    :param file_name: source file name, to specify results for each files in input source
    :return: If all partitions PASS -> True, else False
    """

    def validate_partition(df_batch, partition_info=None):
        # print(df_batch.head())
        print(df_batch.shape)
        res = ge_context.run_checkpoint(
            checkpoint_name="in_memory_stg_checkpoint",
            batch_request={
                "runtime_parameters": {"batch_data": df_batch},
                "data_asset_name": data_asset_name,
                "batch_identifiers": {
                    "default_identifier_name": f"{date_str_prefix}_{file_name}_{partition_info['number']}"
                },
            },
            expectation_suite_name=ge_suite_name,
            run_name_template=f"{date_str_prefix.replace('/', '-')}_{file_name}_%Y%m%d",
        )

        return res

    # check validation results, if 1 among these failed, then whole set fail.
    results = df.map_partitions(validate_partition, meta=('result', 'object')).persist()
    # results = [validate_partition(df.partitions[i].compute(), i) for i in range(df.npartitions)]

    for r in results:
        if not r['success']:
            return False
    return True