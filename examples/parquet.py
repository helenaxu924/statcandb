from contextlib import contextmanager

import duckdb
import pandas as pd
from dotenv import load_dotenv

from statcandb.config import get_config, set_config_from_env

load_dotenv()
set_config_from_env()


@contextmanager
def get_conn():
    config = get_config()
    with duckdb.connect() as conn:
        conn.execute(
            f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_region='auto';
            SET s3_endpoint='{config.aws_endpoint}';
            SET s3_access_key_id='{config.aws_access_key_id}';
            SET s3_secret_access_key='{config.aws_secret_access_key}';
        """
        )
        yield conn


def pull_file() -> pd.DataFrame:
    with get_conn() as conn:
        return conn.execute(
            """
            select *
            from read_parquet('s3://statcandb/14100287.parquet/**/*.parquet', HIVE_PARTITIONING = 1)
            where GEO in ('Nova Scotia', 'Ontario', 'Quebec')
            and Sex <> 'Both sexes'
            and year > 2000
        """
        ).df()


if __name__ == "__main__":
    print(pull_file())
