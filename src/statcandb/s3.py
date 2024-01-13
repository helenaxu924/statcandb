import boto3

from statcandb.config import Config


def get_s3(config: Config):
    return boto3.client(
        "s3",
        endpoint_url=f"https://{config.r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        region_name="auto",
    )
