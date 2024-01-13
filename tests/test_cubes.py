import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pytest_httpserver import HTTPServer

from statcandb.cubes import ProductMetadata, get_pid_list, pull_cube


@pytest.fixture(scope="module")
def all_cubes(fixtures_path: Path) -> list[dict[str, Any]]:
    with open(fixtures_path / "getAllCubesListLite.json", "rt") as infile:
        return json.load(infile)


def test_get_pid_list(httpserver: HTTPServer, all_cubes: list[dict[str, Any]]):
    httpserver.expect_request("/t1/wds/rest/getAllCubesListLite").respond_with_json(
        all_cubes
    )
    pid_list = get_pid_list(httpserver.url_for("/t1/wds/rest/getAllCubesListLite"))
    expected_pid_list = [
        ProductMetadata(product_id=10100001, release_time=datetime(2012, 8, 1, 12, 30)),
        ProductMetadata(
            product_id=10100002, release_time=datetime(2023, 7, 31, 12, 30)
        ),
        ProductMetadata(
            product_id=10100003, release_time=datetime(2023, 7, 25, 12, 30)
        ),
        ProductMetadata(
            product_id=10100004, release_time=datetime(2023, 6, 21, 12, 30)
        ),
        ProductMetadata(
            product_id=10100005, release_time=datetime(2022, 11, 25, 13, 30)
        ),
    ]
    assert pid_list == expected_pid_list


def test_pull_cube(httpserver: HTTPServer, fixtures_path: Path):
    product_id = 10100001
    url_format = "/t1/wds/rest/getFullTableDownloadCSV/{product_id}/en"
    with open(fixtures_path / f"{product_id}-eng.zip", "rb") as infile:
        product_data = infile.read()
    httpserver.expect_request(
        url_format.format(product_id=product_id)
    ).respond_with_json(
        {
            "status": "SUCCESS",
            "object": httpserver.url_for(f"n1/tbl/csv/{product_id}-eng.zip"),
        }
    )
    httpserver.expect_request(f"/n1/tbl/csv/{product_id}-eng.zip").respond_with_data(
        product_data
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        pull_cube(
            product_id,
            download_dir=tmpdir,
            base_url=httpserver.url_for(
                "/t1/wds/rest/getFullTableDownloadCSV/" + r"{product_id}/en"
            ),
        )

        with open(Path(tmpdir) / f"{product_id}-eng.zip", "rb") as infile:
            assert infile.read() == product_data
