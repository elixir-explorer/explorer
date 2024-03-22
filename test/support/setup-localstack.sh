#!/bin/bash
# This script is responsible for starting the "localstack" service.
#
# Along with the service, it creates a bucket and store a parquet
# file there.
#
# This script requires podman or docker, and the aws-cli installed.

# Exit in the first error.
set -e

export AWS_DEFAULT_REGION="us-east-1"
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_ENDPOINT=http://localhost:4566

# Dir discovery
SCRIPT_PATH="${BASH_SOURCE:-$0}"
ABS_SCRIPT_PATH="$(realpath "${SCRIPT_PATH}")"

# The parquet is in the same directory of this script.
FILE_PATH="$(dirname "${ABS_SCRIPT_PATH}")/wine.parquet"

# Check for the container tool.
if command -v podman &> /dev/null;
then
  container_tool="podman"
elif command -v docker &> /dev/null;
then
  container_tool="docker"
else
  echo "Cannot find a compatible container tool. Please install podman or docker"
  exit 1
fi

if ! command -v aws &> /dev/null;
then
  echo "The aws command was not found. Please install the aws-cli."
  echo "See: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
  exit 1
fi

# Run podman or docker.
command "$container_tool" run -d -p 4566:4566 docker.io/localstack/localstack:s3-latest
command "$container_tool" run -d -p 1338:1338 amazon/amazon-ec2-metadata-mock:v1.9.2 --imdsv2
echo "waiting a little bit.."
sleep 5

echo "creating bucket"
# Create the bucket and copy the file to there.
aws --region us-east-1 --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket
sleep 2

echo "uploading file"
aws --region us-east-1 --endpoint-url=http://localhost:4566 s3 cp "$FILE_PATH" s3://test-bucket/wine.parquet
