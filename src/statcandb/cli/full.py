import itertools as its
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Optional

import click
import pyarrow as pa
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm.cli import tqdm

from statcandb.config import get_config
from statcandb.models import Product
from statcandb.s3 import get_s3

from ..cubes import (ProductMetadata, get_changed_pid_list, get_pid_list,
                     process_cube, pull_cube)


@click.group("full")
def full_group():
    """Dealing with full files"""


@full_group.command("download")
@click.option(
    "--download-dir",
    "-d",
    envvar="STATCANDB_DELTA_DOWNLOAD_DIR",
    help="The directory to download delta files to. Defaults to cwd",
    default=None,
)
@click.argument("product_id")
def download_command(product_id: str, download_dir: str | None = None):
    """Download a full file"""
    download_dir = download_dir or Path.cwd()
    pull_cube(product_id, download_dir=download_dir, verbose=True)


@full_group.command("prepare")
@click.argument("filename")
@click.option(
    "--outfile",
    "-o",
    default=None,
    help="Where to write the output",
)
def prepare_command(filename: str, outfile: str | None):
    """Prepare a cube from its ZIP file state to its parquet state"""
    filename = Path(filename)

    if outfile is None:
        outfile = Path.cwd() / "".join(x for x in Path(filename).name if x.isdigit())
        outfile = outfile.with_suffix(".parquet")

    process_cube(filename, outfile)


@full_group.command("push")
@click.argument("path")
def push_command(path: str):
    from statcandb.config import get_config

    config = get_config()
    s3 = get_s3(config)

    path = Path(path)
    product_id = path.name

    objects = s3.list_objects_v2(Bucket=config.r2_bucket, Prefix=f"{product_id}")
    keys = [{"Key": obj["Key"]} for obj in objects.get("Contents", [])]
    if keys:
        s3.delete_objects(Bucket=config.r2_bucket, Delete={"Objects": keys})

    for filepath in path.rglob("*.parquet"):
        if filepath.is_file():
            with tqdm(
                desc=filepath.name,
                total=filepath.stat().st_size,
                unit="iB",
                unit_scale=True,
                unit_divisor=1_024,
                leave=False,
            ) as inner_pbar:
                s3.upload_file(
                    str(filepath),
                    config.r2_bucket,
                    f"{product_id}/{filepath.relative_to(path)}",
                    Callback=lambda b: inner_pbar.update(b),
                )


def _pull_process_upload_cube(
    product: ProductMetadata, s3, bucket_name: str, session
) -> bool:
    product_id = product.product_id
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        try:
            pull_cube(product_id, download_dir=tmpdir, verbose=True)
        except requests.exceptions.HTTPError:
            click.echo(f"Problem downloading {product_id}. Continuing")
            return False

        path = tmpdir / f"{product_id}.parquet"
        try:
            process_cube(tmpdir / f"{product_id}-eng.zip", path)
        except zipfile.BadZipFile:
            click.echo(f"Bad zip file for {product_id}. Continuing")
            return False
        except pa.lib.ArrowInvalid:
            click.echo(
                f"The CSV appears to be badly constructed for {product_id}. Continuing"
            )
            return False

        product_id_path = path.name

        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{product_id_path}/")
        keys = [{"Key": obj["Key"]} for obj in objects.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": keys})

        for filepath in tqdm(
            list(path.rglob("*.parquet")), desc="Uploading files", leave=False
        ):
            if filepath.is_file():
                with tqdm(
                    desc=filepath.name,
                    total=filepath.stat().st_size,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1_024,
                    leave=False,
                ) as inner_pbar:
                    s3.upload_file(
                        str(filepath),
                        bucket_name,
                        f"{product_id_path}/{filepath.relative_to(path)}",
                        Callback=lambda b: inner_pbar.update(b),
                    )

        session.add(Product(product_id, product.release_time, is_uploaded=True))
        session.commit()
        return True


@contextmanager
def get_session(session: Optional[Any]):
    if session is not None:
        yield session
    else:
        config = get_config()
        engine = create_engine(config.sqlite_db_url)
        Session = sessionmaker(engine)
        with Session() as session:
            yield session


def _pull_process_upload_cube_list(
    products: list[ProductMetadata], skip: list[str] = [], session: Optional[Any] = None
) -> int:
    config = get_config()
    s3 = get_s3(config)

    skip = [int(x) for x in skip]
    success_count = 0
    with get_session(session) as session:
        for product in (pbar := tqdm(products)):
            product_id = product.product_id
            if product_id in skip:
                click.echo(f"Skipping {product_id}")
                continue
            pbar.set_description(f"{product_id}")
            success_count += _pull_process_upload_cube(
                product, s3, config.r2_bucket, session
            )
    return success_count


@full_group.command("delta")
@click.option("--start-date", "-s", type=str, default=None)
@click.option("--end-date", "-e", type=str, default=None)
@click.option("--skip", "-k", type=int, multiple=True, default=[])
def delta_command(start_date: Optional[str], end_date: Optional[str], skip: List[str]):
    """
    Only pull updated cubes between start-date and end-date (inclusive)

    If start-date or end-date is not given, it defaults to today's date
    Dates are formated as YYYY-MM-DD
    """
    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        start_date = datetime.now().date()

    if end_date:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end_date = datetime.now().date()

    this_date = start_date
    product_ids: List[ProductMetadata] = []
    while this_date <= end_date:
        product_ids.extend(get_changed_pid_list(this_date))
        this_date += timedelta(days=1)

    # Keep only the _latest_ product id in case we get a range of them
    product_ids = sorted(set(product_ids))
    product_ids = [
        max(list(products), key=lambda p: p.release_time)
        for _, products in its.groupby(product_ids, lambda p: p.product_id)
    ]

    click.echo(f"Pulling {len(product_ids)} product_ids")
    success_count = _pull_process_upload_cube_list(product_ids, skip)
    click.echo(
        f"Successfully uploaded {success_count} out of {len(product_ids)} products"
    )


@full_group.command("delta-by-diff")
@click.option("--skip", "-k", type=int, multiple=True, default=[])
@click.option(
    "--max-product-ids",
    type=int,
    default=0,
    help="Maximum number of products to upload before quitting",
)
@click.option(
    "--start-from",
    type=int,
    default=0,
    help="Only download product ids which are at least this number",
)
def delta_by_diff_command(skip: List[str], max_product_ids: int, start_from: int):
    """
    Pull all cubes which were either:
        * Not uploaded; or
        * Have a release time in our database earlier than statcan's
          metadata's release time
    """
    config = get_config()
    skip = [int(x) for x in skip]
    start_from = int(start_from)

    # Get all products
    all_product_ids = get_pid_list()

    # TODO: If there are ever more than a few thousand of these, would need to
    # do this in batch but :shrug: for now
    engine = create_engine(config.sqlite_db_url)
    Session = sessionmaker(engine)
    with Session() as session:
        uploaded_product_ids = session.query(Product).all()
        uploaded_p_to_r = {val.number: val.release_time for val in uploaded_product_ids}

        to_pull = sorted(
            product
            for product in all_product_ids
            if (product.product_id >= start_from)
            and (
                (product.product_id not in uploaded_p_to_r)
                or (product.release_time > uploaded_p_to_r[product.product_id])
            )
        )

        click.echo(f"Need to pull {len(to_pull)} product ids")
        if max_product_ids > 0 and len(to_pull) > max_product_ids:
            to_pull = to_pull[:max_product_ids]
            click.echo(
                f"More products than allowed to upload. Uploading only {max_product_ids}"
            )

        success_count = _pull_process_upload_cube_list(to_pull, skip, session=session)
        click.echo(
            f"Successfully uploaded {success_count} out of {len(to_pull)} products"
        )
