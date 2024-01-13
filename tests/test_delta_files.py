import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
import pytest_httpserver

from statcandb.delta_files import pull_delta_file, split_delta_file


@pytest.fixture(scope="module")
def delta_file_data(fixtures_path: Path) -> bytes:
    with open(fixtures_path / "20230803.zip", "rb") as infile:
        return infile.read()


def test_pull_delta_file(
    httpserver: pytest_httpserver.HTTPServer, delta_file_data: bytes
):
    httpserver.expect_request("/n1/delta/20230803.zip").respond_with_data(
        delta_file_data
    )

    # Get correct URL format string for mocking
    url = httpserver.url_for("/n1/delta/20230803.zip")
    url = url[: -len("20230803.zip")] + "{download_date}.zip"

    with tempfile.TemporaryDirectory() as tmpdir:
        download_path = pull_delta_file(datetime(2023, 8, 3), tmpdir, base_url=url)
        with open(download_path, "rb") as infile:
            assert infile.read() == delta_file_data


def test_split_delta_files(fixtures_path: Path):
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        split_delta_file(
            fixtures_path / "20230715.csv",
            tmpdir,
            format="parquet",
        )

        assert (tmpdir / "productId=23100309").exists()
        assert (tmpdir / "productId=23100309").is_dir()

        # Check that the set of values and coordinates match
        left_df = pd.read_csv(fixtures_path / "20230715.csv")
        right_df = pd.read_parquet(tmpdir / "productId=23100309" / "part-0.parquet")
        assert Counter(left_df["coordinate"]) == Counter(right_df["coordinate"])
        assert Counter(left_df["value"].fillna(-1)) == Counter(
            right_df["value"].fillna(-1)
        )
