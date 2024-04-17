import duckdb
import polars as pl
import lancedb
import time

from degen_tracker.lance import LanceDBLogs

pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")

# Example that demonstrates how to resume a sync based on the latest block number in the logs database.
# This is useful for constantly keeping the database in sync
lance_logs = LanceDBLogs()

while True:
    # open lancedb table
    uri: str = "logs"
    db: lancedb.DBConnection = lancedb.connect(uri)
    logs_tbl = db.open_table("logs")

    ds = logs_tbl.to_lance()

    row_count = logs_tbl.count_rows()

    print("row count:", row_count)
    most_recent_block: int = duckdb.query(
        """
    SELECT 
    max(block_number)
    FROM ds
    """
    ).fetchall()[0][0]

    print('most recent block', most_recent_block)
    lance_logs.db_sync(start_block=most_recent_block,
                       end_block=None, block_chunks=25)
    time.sleep(5)
