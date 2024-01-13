import csv
import shutil
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Union

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests

from statcandb.file_utils import download_file, unzip_file

BASE_URL_ALL_CUBES = "https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesListLite"
BASE_URL_FULL_TABLE = (
    "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{product_id}/en"
)
BASE_URL_CHANGED_CUBES = (
    "https://www150.statcan.gc.ca/t1/wds/rest/getChangedCubeListLite/{yyyymmdd}"
)


@dataclass(frozen=True, order=True)
class ProductMetadata:
    product_id: int
    release_time: datetime


def get_pid_list(base_url: str = BASE_URL_ALL_CUBES) -> list[ProductMetadata]:
    """
    Args:
        base_url: The URL to use to pull the list of all cubes

    Returns:
        The list of available product ids from the statcan web data service
    """
    response = requests.get(base_url)
    response.raise_for_status()
    return [
        ProductMetadata(
            int(val["productId"]),
            datetime.strptime(val["releaseTime"], "%Y-%m-%dT%H:%M"),
        )
        for val in response.json()
    ]


def get_changed_pid_list(
    the_date: Union[str, datetime, date], base_url: str = BASE_URL_CHANGED_CUBES
) -> list[ProductMetadata]:
    """
    Get list of products changed on the specified date
    """
    the_date = the_date if isinstance(the_date, str) else the_date.strftime("%Y-%m-%d")
    url = base_url.format(yyyymmdd=the_date)
    response = requests.get(url)
    response.raise_for_status()
    [
        ProductMetadata(
            int(val["productId"]),
            datetime.strptime(val["releaseTime"], "%Y-%m-%dT%H:%M"),
        )
        for val in response.json()
    ]


def pull_cube(
    product_id: int,
    download_path: Optional[str | Path] = None,
    download_dir: Optional[str | Path] = None,
    base_url: str = BASE_URL_FULL_TABLE,
    verbose: bool = False,
):
    response = requests.get(base_url.format(product_id=product_id))
    response.raise_for_status()
    url = response.json()["object"]
    download_file(
        url, download_path=download_path, download_dir=download_dir, verbose=verbose
    )


def prep_cube(tab: pa.Table) -> pa.Table:
    val_arr = tab["VALUE"]
    tab = tab.drop_columns(["DECIMALS", "UOM_ID", "SCALAR_FACTOR", "VALUE"])
    tab = tab.append_column(pc.multiply(val_arr, pc.power(10, tab["SCALAR_ID"])))
    return tab


def transform_files(folder_path: str | Path):
    folder_path = Path(folder_path)
    for file_name in Path(folder_path).rglob("*.csv"):
        tab = prep_cube(pcsv.read_csv(file_name))
        pq.write_table(tab, file_name.with_suffix(".parquet"))


def process_cube(filename: str | Path, outfile: str | Path):
    filename = Path(filename)
    outfile = Path(outfile)
    with unzip_file(filename) as csv_path:
        # Sometimes, the CSV has the same name several times (usually "Symbols")
        # Fix this in a mildly hacky way
        with open(csv_path, "rt") as infile:
            # Sometimes, the CSVs come with strange non-printable starting characters
            # We remove them from consideration and then start our csv reader from there
            read = 0
            while b := infile.read(1):
                if ord(b) < 128:
                    # Sometimes, the columns are quoted, and sometimes they are not :/
                    # So check if the character is an ascii character
                    # (The first column is always REF_DATE, so will be either
                    # `"REF_DATE"` or `REF_DATE`)
                    break
                read += len(b.encode("utf8"))

            infile.seek(read)

            reader = csv.reader(infile)
            names = next(reader)
            column_names = []
            for name in names:
                new_name = name
                counter = 1
                while new_name in column_names:
                    new_name = f"{name}.{counter}"
                    counter += 1
                column_names.append(new_name)

        file_format = ds.CsvFileFormat(
            read_options=pcsv.ReadOptions(
                skip_rows=1,
                column_names=column_names,
            )
        )

        d = ds.dataset(csv_path, format=file_format)
        schema = d.schema
        columns = {name: ds.field(name) for name in schema.names}
        columns["year"] = pc.utf8_slice_codeunits(
            ds.field("REF_DATE").cast(pa.string()), 0, 4
        )

        # For some reason, the 98 series has a _totally_ different format than all others,
        # so we only do this if the column exists
        if "DECIMALS" in column_names:
            max_decimals = pc.max(
                d.scanner(columns=["DECIMALS"]).to_table()["DECIMALS"]
            ).as_py()
        else:
            max_decimals = None

        new_schema_list = []
        for name, type in zip(schema.names, schema.types):
            if (name in ["STATUS", "SYMBOL", "TERMINATED", "DGUID"]) or (
                name[:6] == "Symbol"
                and ((len(name) < 7) or (name[6] == "." and name[7:].isdigit()))
            ):
                # These fields are usually _mostly_ null but should be strings
                new_schema_list.append(pa.field(name, pa.string()))
            elif name == "VALUE":
                # The VALUE column seems to appear only if DECIMALS appears,
                # so this should be OK
                if max_decimals > 0:
                    new_schema_list.append(pa.field(name, pa.float64()))
                else:
                    new_schema_list.append(pa.field(name, pa.int64()))
            else:
                new_schema_list.append(pa.field(name, type))
        new_schema = pa.schema(new_schema_list)

        d = ds.dataset(csv_path, format=file_format, schema=new_schema)
        s = d.scanner(columns=columns)

        # TODO: Improve this file size test so we only write once
        ds.write_dataset(
            s,
            outfile,
            format="parquet",
            partitioning=["year"],
            partitioning_flavor="hive",
        )

        # If the total size is < 10 MiB, then don't bother partitioning
        total_size = sum(f.stat().st_size for f in outfile.rglob("*") if f.is_file())
        if total_size < 10 * 1_024 * 1_024:  # 10 MiB
            shutil.rmtree(outfile)
            s = d.scanner(columns=columns)
            ds.write_dataset(
                s,
                outfile,
                format="parquet",
            )
