from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Union

import duckdb
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.dataset as ds
import requests

from statcandb.file_utils import download_file

# Not sure how to use this yet :/
# scalar_factor = int(row[scalar_factor_column])
# if delta_value == "":
#     # this handles if value is empty
#     delta_value = 0.0
# else:
#     delta_value = float(delta_value) * math.pow(
#         10, scalar_factor
#     )  # Helena: note here that this computation-wise works!
#     # however, not sure if it messes with schema since the value is eg,
#     #  114512000000.0 after scalar (extra .0 at end)
#     # but, my interpretation (helena) is that value is still a float so the
#     # extra .0 doesn't affect anything (hopefully)


BASE_URL = "https://www150.statcan.gc.ca/n1/delta/{download_date}.zip"

# schema pulled from https://www.statcan.gc.ca/en/developers/df/user-guide#a09
DELTA_FILE_SCHEMA = pa.schema(
    [
        ("productId", pa.int64()),  # max 10 digits requires int64
        ("coordinate", pa.string()),
        ("vectorId", pa.int64()),  # 10 digits requires int64
        ("refPer", pa.string()),
        ("refPer2", pa.string()),
        ("symbolCode", pa.uint8()),
        ("statusCode", pa.uint8()),
        ("securityLevelCode", pa.uint8()),
        ("value", pa.float64()),
        ("releaseTime", pa.string()),  # Actually a date in format YYYY-MM-DDTHH:MM
        ("scalarFactorCode", pa.uint8()),
        ("decimals", pa.uint8()),
        ("frequencyCode", pa.uint8()),
    ]
)


def pull_delta_file(
    download_date: Union[str, datetime, date],
    download_dir: Union[str, Path],
    session: Optional[requests.Session] = None,
    base_url: str = BASE_URL,
) -> Path:
    """
    Download a delta file from the StatCan website

    Args:
        download_date: The date of the delta file to download. If it is
            a str, it must be in the format YYYYMMDD
        download_dir: The directory to download the delta file to
        session: Optionally, a requests.Session to use to download the file
        base_url: The format string to use when constructing the delta file URL
            Used primarily for mocking

    Returns:
        The path to the downloaded file
    """
    if not isinstance(download_date, str):
        download_date = download_date.strftime("%Y%m%d")

    delta_file_url = base_url.format(download_date=download_date)
    download_dir = Path(download_dir)

    if not download_dir.exists():
        raise ValueError(f"download_dir must exist: {download_dir}")

    return download_file(delta_file_url, download_dir=download_dir, session=session)


def split_delta_file(
    delta_file_path: Union[str, Path], workdir: Union[str, Path], format: str = "arrow"
):
    """
    Split a delta file (which is a single CSV) into a hive-partitioned data set.

    Args:
        delta_file_path: The path to the delta file CSV
        workdir: The directory to save the data set in. MUST NOT EXIST
        format: The format to store the data set in. For temporary work, we suggest
            "arrow" as it will allow you to skip a lot of computation in
            reading and writing. For long term storage, use "parquet"
    """
    cro = pcsv.ReadOptions(
        skip_rows=1,
        column_names=DELTA_FILE_SCHEMA.names,
        encoding="utf8",
    )
    cco = pcsv.ConvertOptions(column_types=DELTA_FILE_SCHEMA)

    with closing(
        pcsv.open_csv(delta_file_path, read_options=cro, convert_options=cco)
    ) as batches:
        ds.write_dataset(
            batches,
            workdir,
            partitioning=ds.partitioning(
                flavor="hive", schema=pa.schema([("productId", pa.int64())])
            ),
            format=format,
        )


def merge_delta_file(
    original_path: Union[str, Path],
    delta_path: Union[str, Path],
    new_path: Union[str, Path],
):
    """
    Merge a delta file into an old data set.

    NOTE: This operation is performed _entirely in memory_ so if a data set
    is particularly large, this might fail. However, we haven't hit this issue yet

    TODO: Figure out how partitions are supposed to happen

    Args:
        original_path: The path of the data being modified
        delta_path: The path to the delta data
        new_path: The path to write the new data to
    """
    with duckdb.connect(":memory:", read_only=False) as con:
        # creating temp table and copying everything
        con.execute(
            f"CREATE TEMPORARY TABLE original_table AS SELECT * FROM '{original_path}'"
        )
        con.execute(f"CREATE TEMPORARY TABLE new_table AS SELECT * FROM '{delta_path}'")

        # update rows in specified table using provided values
        con.execute(
            """
            UPDATE original_table
            SET value = new_table.value
            FROM new_table
            WHERE original_table.vectorId = new_table.vectorId
        """
        )

        # write updated data back to original parquet
        con.execute(
            f"COPY (SELECT * FROM original_table) TO '{new_path}' (FORMAT 'parquet')"
        )
