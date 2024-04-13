import datetime
import lancedb
import polars as pl
import time

from dataclasses import dataclass
from degen_tracker.hypersync import Hypersync


@dataclass
class LanceDBLogs:
    # 1. connect to lancedb and initialize a lance table an initial query
    uri: str = "logs"
    db: lancedb.DBConnection = lancedb.connect(uri)
    # error when trying to create table, it already exists.
    logs_tbl = None

    def __post_init__(self):
        # Check if database exists. If it does, then update logs_tbl to point to the existing database
        if self.db is None:
            self.logs_tbl = None
        else:
            self.logs_tbl = self.db.open_table(self.uri)

    def initial_db_sync(self, full_sync=False):
        """
        Call this function first to create the database and sync the logs table. 

        full_sync=False defaults to a 10000 block range sync. Use this to get started.
        full_sync=True if you want to sync the entire history.

        Use full_sync=False
        """
        client = Hypersync()

        match full_sync:
            case True:
                erc20_logs_df: pl.DataFrame = client.get_erc20_df(
                    sync_all=full_sync)
            case False:
                # sync to head with a 1000 range
                erc20_logs_df: pl.DataFrame = client.get_erc20_df(
                    sync_all=full_sync, block_num_range=10000)

        try:
            # Attempt to create the table if it doesn't exist
            self.logs_tbl = self.db.create_table(
                self.uri, data=erc20_logs_df)
        except OSError as e:
            # Check if the error message is about the dataset already existing
            if "Dataset already exists" in str(e):
                # Skip table creation because it already exists
                print("Table already exists, skipping creation.")
            else:
                # If the error is due to another reason, re-raise the exception
                raise

    def update_db(self, refresh_rate: int = 5):
        """
        Constantly streams new data to update the logs database.
        `refresh_rate` is the number of seconds to wait before the next call.

        This function assumes that the initial_db_sync has already been called.
        """
        client = Hypersync()

        while True:
            # TODO - make it so it reads the latest block and creates a dynamic range.
            erc20_logs_df: pl.DataFrame = client.get_erc20_df()

            # Perform a "upsert" operation
            self.logs_tbl.merge_insert("block_number")   \
                .when_not_matched_insert_all() \
                .execute(erc20_logs_df)

            # lance db cleanup
            # make table fragments compact
            # 1m target rows per file
            self.logs_tbl.compact_files(target_rows_per_fragment=1000000)

            self.logs_tbl.cleanup_old_versions(
                older_than=datetime.timedelta(seconds=30), delete_unverified=True
            )

            print(f'sleeping for {refresh_rate} seconds')
            time.sleep(refresh_rate)
