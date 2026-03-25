import botocore, logging, os, boto3
from botocore.exceptions import ClientError
from botocore.client import Config


def create_prefix(bucket_name=None, prefix=None, s3_client=None):
    bucket_exists = False
    try:
        response = s3_client.head_bucket(Bucket=bucket_name)
        bucket_exists = True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            bucket_exists = False

    # Create bucket if it doesn't exist
    if not bucket_exists:
        s3_client.create_bucket(Bucket=bucket_name)

    # Create prefix if it doesn't exist
    prefix_exists = False
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" in response:
        prefix_exists = True

    if not prefix_exists:
        logging.debug(f"{bucket_name}/{prefix} is being created")
        if prefix.endswith("/"):
            prefix = prefix[:-1]
        s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/")


def object_exists(s3_client, bucket_name, key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise


if __name__ == "__main__":

    session = boto3.Session(
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", None),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", None),
    )

    s3 = session.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_URL", None),
        config=Config(signature_version="s3v4"),
    )

    # Example usage - replace bucket_name with actual bucket
    create_prefix(bucket_name=os.environ.get("S3_BUCKET", "ws-analytics-bucket"), prefix="laliga/10317", s3_client=s3)

    stop = True
