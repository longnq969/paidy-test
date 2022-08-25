import great_expectations as ge
from great_expectations.core.batch import BatchRequest
import datetime
from pandas import DataFrame
from scripts.common.helpers import DATE_FORMAT


def validate_data_from_s3_data_source(checkpoint_name: str,
                                      data_asset_name: str,
                                      ge_suite_name: str,
                                      date_str: str = None) -> bool:

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
            }
        }
    )

    results = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        # validations={
        #     "batch_request": batch_request,
        #     "expectation_suite_name": ge_suite_name
        # },
        batch_request=batch_request,
        expectation_suite_name=ge_suite_name,
        run_name_template=f"{data_asset_name}_{date_str}_%Y%m%d",

    )
    print(results)

    return True


def validate_data_in_memory(data: DataFrame, checkpoint_name: str) -> bool:

    return True
