# Statcan DB

A case study in accessing public data via flat parquet files and duckdb.

## Requirements

This repository was built with python 3.11. It may work with older versions, but there are no guarantees.

Requirements are managed with [poetry](https://python-poetry.org/). If poetry is installed, then you can run

```bash
poetry install
```

to install all requirements. Then you can access the CLI either by activating the virtualenv

```bash
poetry shell
```

or via using the `poetry run` prefix.

## Setup

You will need to create a `.env` file with several credentials. To begin, you can run

```bash
cp .env.sample .env
```

### R2-related Variables

We use [Cloudflare's R2 service](https://www.cloudflare.com/developer-platform/r2/) to store all of our data. The following environment comes from this service:

  * `R2_ACCOUNT_ID`
  * `R2_BUCKET`
  * `AWS_ACCESS_KEY_ID`
  * `AWS_SECRET_ACCESS_KEY`

In order to use this software locally, you will need to setup a [Cloudflare](https://www.cloudfare.com/) account and create an R2 bucket in the [Cloudflare dashboard](https://dash.cloudflare.com/).

Once this bucket is created, you will need to create an access key, which you can do by following [these instructions](https://developers.cloudflare.com/r2/api/s3/tokens/).

Now that these are setup you can find these variables in the following locations:

  * `R2_ACCOUNT_ID`: If you go to `dash.cloudflare.com` and log in, the URL should look like `dash.cloudflare.com/<<YOUR ACCOUNT ID>>`. It should look like a long hex string.
  * `R2_BUCKET`: This is the name of the bucket you created.
  * `AWS_ACCESS_KEY_ID`: See the instructions above.
  * `AWS_SECRET_ACCESS_KEY`: See the instructions above.

### Sqlite database

For the `SQLITE_DB_URL` you will need to specify a location to store the sqlite database that tracks our downloads. This is a [SQLAlchemy URL](https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#pysqlite), so it should follow the format required by SQLAlchemy.

The easiest way to get started is to set this to `sqlite:///db.db`, which will store the database in the root directory of the project called `db.db`.

### US Census

This is not required for running the main repository, but is used in [the US Census example](examples/census.py). To get a key, go to [this website](https://api.census.gov/data/key_signup.html).

## The CLI

To populate the parquet files, run

```bash
statcandb full delta-by-diff
```

## Examples

Once you've run [the CLI](#the-cli) you should now be able to use the [parquet example](examples/parquet.py).

## License

MIT
