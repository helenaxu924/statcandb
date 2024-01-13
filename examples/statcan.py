import io
import sys
import zipfile

import pandas as pd
import requests

BASE_URL = (
    "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{table_id}/en"
)


def pull_table(table_id: int) -> pd.DataFrame:
    """
    Given a table id, return a data frame with its full contents.

    For example, to pull the population of all provinces, call::

        pull_table(17100009)
    """
    resp = requests.get(BASE_URL.format(table_id=table_id))
    resp.raise_for_status()
    j = resp.json()
    if not j["status"] == "SUCCESS":
        raise EnvironmentError(f"The status of {table_id=} was not `SUCCESS`")

    resp = requests.get(j["object"])
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zfile:
        content = zfile.read(f"{table_id}.csv")
        return pd.read_csv(io.BytesIO(content))


if __name__ == "__main__":
    print(pull_table(int(sys.argv[1])))
