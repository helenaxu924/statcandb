import csv
import tempfile
from pathlib import Path

from pytest_httpserver import HTTPServer

from statcandb.file_utils import download_file, unzip_file


def test_download_file(httpserver: HTTPServer):
    httpserver.expect_request("/foo.csv").respond_with_data(b"foobar")

    with tempfile.TemporaryDirectory() as tmpdir:
        download_path = download_file(
            httpserver.url_for("/foo.csv"), download_dir=tmpdir
        )
        assert download_path == Path(tmpdir) / "foo.csv"
        with open(download_path, "rb") as infile:
            assert infile.read() == b"foobar"

    with tempfile.TemporaryDirectory() as tmpdir:
        to_download_path = Path(tmpdir) / "tmp.csv"
        download_path = download_file(
            httpserver.url_for("/foo.csv"), download_path=to_download_path
        )
        assert download_path == to_download_path
        with open(download_path, "rb") as infile:
            assert infile.read() == b"foobar"


def test_unzip_file(fixtures_path: Path):
    with unzip_file(fixtures_path / "20230803.zip") as csv_path:
        assert csv_path.name == "20230803.csv"
        with open(csv_path, "rt") as infile:
            reader = csv.reader(infile)
            assert next(
                reader
            ) == "productId,coordinate,vectorId,refPer,refPer2,symbolCode,statusCode,securityLevelCode,value,releaseTime,scalarFactorCode,decimals,frequencyCode".split(
                ","
            )

    with unzip_file(fixtures_path / "20230715.csv") as csv_path:
        assert csv_path.name == "20230715.csv"
        with open(csv_path, "rt") as infile:
            reader = csv.reader(infile)
            assert next(
                reader
            ) == "productId,coordinate,vectorId,refPer,refPer2,symbolCode,statusCode,securityLevelCode,value,releaseTime,scalarFactorCode,decimals,frequencyCode".split(
                ","
            )
