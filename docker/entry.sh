#!/bin/sh

cd /app
chmod +x mc

# connect to minio
./mc alias set minio http://minio:9000 minio abc13579 --api S3v4

# create buckets
./mc mb minio/raw-zone
./mc mb minio/staging-zone
./mc mb minio/golden-zone
./mc mb minio/insight-zone
./mc mb minio/data-validation
./mc mb minio/data-validation/expectations
./mc mb minio/data-validation/checkpoints
./mc mb minio/data-validation/validation-results
./mc cp -r ./great_expectations/expectations/ minio/data-validation/expectations/
./mc cp -r ./great_expectations/checkpoints/ minio/data-validation/checkpoints/