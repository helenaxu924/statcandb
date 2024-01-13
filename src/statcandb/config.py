import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    aws_access_key_id: str
    aws_secret_access_key: str
    r2_account_id: str
    r2_bucket: str
    aws_endpoint: str
    sqlite_db_url: str


config = None


def set_config_from_env():
    global config
    config = Config(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        r2_account_id=os.environ.get("R2_ACCOUNT_ID"),
        r2_bucket=os.environ.get("R2_BUCKET"),
        aws_endpoint=f"{os.environ.get('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        sqlite_db_url=os.environ.get("SQLITE_DB_URL"),
    )


def get_config() -> Config:
    return config
