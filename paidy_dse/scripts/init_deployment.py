from flows.etl import etl_flow
from prefect.deployments import Deployment
import click


@click.command()
@click.option("--source-folder", type=str, default="./data/sources", help="Input folder, default is './data/sources'")
@click.option("--interval", type=int, default=86400, help="Flow run interval, by second, default is daily")
@click.option("--timezone", type=str, default="UTC", help="Timezone, default is UTC")
def create_deployment(source_folder: str, interval: int, timezone: str):

    deployment = Deployment.build_from_flow(
        flow=etl_flow,
        name="etl_daily",
        version="1",
        tags=["paidy"],
        parameters={"source_folder": source_folder},
        schedule={
            "interval": interval,
            "timezone": timezone
        }
    )

    deployment.apply()


if __name__ == "__main__":
    create_deployment()