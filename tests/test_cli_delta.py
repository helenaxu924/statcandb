import shutil
import tempfile
import zipfile
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.dataset as ds
from click.testing import CliRunner

from statcandb.cli import delta


def test_split_zip(fixtures_path: Path):
    runner = CliRunner()
    zip_path = fixtures_path / "20230803.zip"
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(delta.split_command, [str(zip_path), tmpdir])
        assert result.exit_code == 0

        with zipfile.ZipFile(zip_path) as zf:
            with zf.open("20230803.csv") as zf_csv:
                df = pd.read_csv(zf_csv)

        d = ds.dataset(tmpdir, partitioning="hive")
        orig_df = duckdb.query("SELECT DISTINCT productId FROM d").df()
        assert set(orig_df["productId"].values) == set(df["productId"].values)

        assert d.count_rows() == len(df)


def test_split_csv(fixtures_path: Path):
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        csv_path = tmpdir / "20230803.csv"
        with zipfile.ZipFile(fixtures_path / "20230803.zip") as zf:
            with zf.open("20230803.csv") as zf_csv, open(csv_path, "wb") as outfile:
                shutil.copyfileobj(zf_csv, outfile)

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(delta.split_command, [str(csv_path), tmpdir])
            assert result.exit_code == 0

            df = pd.read_csv(csv_path)

            d = ds.dataset(tmpdir, partitioning="hive")
            orig_df = duckdb.query("SELECT DISTINCT productId FROM d").df()
            assert set(orig_df["productId"].values) == set(df["productId"].values)

            assert d.count_rows() == len(df)
