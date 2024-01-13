import zipfile
from pathlib import Path
from typing import Optional

import click

from statcandb.delta_files import pull_delta_file, split_delta_file


@click.group("delta")
def delta_group():
    """Dealing with delta files"""


@delta_group.command("download")
@click.argument("download_date")
@click.option(
    "--download-dir",
    "-d",
    envvar="STATCANDB_DELTA_DOWNLOAD_DIR",
    help="The directory to download delta files to. Defaults to cwd",
    default=None,
)
def download_command(download_date: str, download_dir: Optional[str] = None):
    """Download a delta file"""
    if not len(download_date) == 8 and download_date.isdigit():
        raise click.BadArgumentUsage("download_date must be in the format YYYYMMDD")

    download_dir = download_dir or Path.cwd()
    pull_delta_file(download_date, download_dir)


@delta_group.command("split")
@click.argument("infile")
@click.argument("outfile")
@click.option(
    "--format", "-f", default="parquet", help="The format of the partitioned file"
)
def split_command(infile: str, outfile: str, format: str):
    """Download a delta file"""
    if infile.lower().endswith(".zip"):
        pinfile = Path(infile)
        csv_filename = pinfile.with_suffix(".csv").name
        with zipfile.ZipFile(infile) as zf:
            with zf.open(csv_filename) as zf_csv:
                split_delta_file(zf_csv, outfile, format=format)
    else:
        split_delta_file(infile, outfile, format=format)
