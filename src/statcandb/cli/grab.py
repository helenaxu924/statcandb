import duckdb

with duckdb.connect() as conn:
    conn.execute("SET s3_endpoint='{s3_endpoint}")
    conn.execute("SET s3_access_key_id='{s3_access_key_id}")
    conn.execute("SET s3_secret_access_key='{s3_secret_access_key}")
    conn.execute("SET s3_region='auto'")
    df = conn.query(
        "SELECT * FROM read_parquet('s3://statcandb/product_id.parquet/*/*.parquet', HIVE_PARTITIONING = 1) WHERE year = 2013"
    ).df()
