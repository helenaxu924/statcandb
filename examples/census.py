import os
import sys

import pandas as pd
import requests

BASE_URL = "https://api.census.gov/data/2022/acs/acs1?get={variables}&for={geo_filter}&key={api_key}"


def pull_table(variables: str, geo_filter: str, api_key: str) -> pd.DataFrame:
    """
    Pull the requested data from the Census API as a data frame

    As an example call, to get the population of every state, call::

        pull_file(
            "NAME,group(B01001)",
            "state:*",
            "YOUR API KEY",
        )
    """
    resp = requests.get(
        BASE_URL.format(
            variables=variables,
            geo_filter=geo_filter,
            api_key=api_key,
        )
    )
    resp.raise_for_status()

    j = resp.json()
    return pd.DataFrame.from_records(j[1:], columns=j[0])


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    print(
        pull_table(
            sys.argv[1],
            sys.argv[2],
            os.environ.get("CENSUS_API_KEY", sys.argv[3]),
        )
    )
