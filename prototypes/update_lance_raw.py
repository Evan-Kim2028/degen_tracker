import datetime
import lancedb
import polars as pl
import time

from degen_tracker.hypersync import Hypersync


client = Hypersync()
erc20_logs_df: pl.DataFrame = client.get_erc20_df()

# 1. connect to lancedb and initialize a lance table an initial query
uri: str = "logs"
db: lancedb.DBConnection = lancedb.connect(uri)
# error when trying to create table, it already exists.
tbl = db.create_table(uri, data=erc20_logs_df, mode="overwrite")


while True:
    erc20_logs_df: pl.DataFrame = client.get_erc20_df()

    # Perform a "upsert" operation
    tbl.merge_insert("block_number")   \
        .when_not_matched_insert_all() \
        .execute(erc20_logs_df)

    # lance db cleanup
    # make table fragments compact
    # 1m target rows per file
    print('cleanup?')
    tbl.compact_files(target_rows_per_fragment=1000000)

    tbl.cleanup_old_versions(
        older_than=datetime.timedelta(seconds=30), delete_unverified=True
    )

    print(tbl.to_pandas().sort_values("block_number").tail(3))

    print('sleeping for 5 seconds')
    time.sleep(5)  # Wait for 15 seconds before the next call
