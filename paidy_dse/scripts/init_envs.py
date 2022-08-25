from prefect.blocks.system import JSON
from prefect.filesystems import RemoteFileSystem


if __name__ == "__main__":
    minio_block = RemoteFileSystem(
        basepath="s3://sources",
        settings={
            "key": "minio",
            "secret": "abc13579",
            "client_kwargs": {"endpoint_url": "http://host.docker.internal:9000"},
        },
    )
    minio_block.save("sources", overwrite=True)