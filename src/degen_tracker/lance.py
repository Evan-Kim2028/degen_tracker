import asyncio
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
    logs_tbl = None

    def __post_init__(self):
        # Check if database exists. If it does, then update logs_tbl to point to the existing database
        if self.db is None:
            self.logs_tbl = None
        else:
            try:
                # Try to open the existing table
                self.logs_tbl = self.db.open_table(self.uri)
            except FileNotFoundError:
                # Handle the case where the table does not exist
                print(
                    f"Table {self.uri} does not exist. Consider creating it if this is expected.")
                self.logs_tbl = None

    def db_sync(self, start_block: int, end_block: int = None, block_chunks=25000):
        """
        Initializes the database and syncs the logs table based on the specified sync range.

        Parameters:
        - full_sync (bool): If True, synchronizes the entire history. If False, synchronizes the last 5000 blocks.
        Defaults to False, which syncs a 5000 block range for initial setup.

        Example:
        - Use full_sync=True to sync the entire history.
        - Use full_sync=False to perform a quick initial sync of the last 5000 blocks.
        """
        client = Hypersync()

        # block height is most recent block height if not specified
        if end_block is None:
            end_block: int = asyncio.run(client.get_block_height())

        for block in range(start_block, end_block, block_chunks):
            erc20_logs_df = None
            if block + block_chunks > end_block:
                print('break!')
                break

            progress_percent = (block / end_block) * 100
            print('progress: ', round(progress_percent, 3),
                  '%', 'block ', block, "/", end_block)
            erc20_logs_df = client.get_erc20_df(
                start_block=block, end_block=block+block_chunks)

            print('rows being added:', erc20_logs_df.shape)
            self.create_db(erc20_logs_df)

            # update db based on chunked info
            self.update_db(erc20_logs_df)

    def create_db(self, df: pl.DataFrame):
        """
        Attempt to create initial lancedb if it doesn't exist.
        """
        try:
            # Attempt to create the table if it doesn't exist
            self.logs_tbl = self.db.create_table(
                self.uri, data=df)
        except OSError as e:
            # Check if the error message is about the dataset already existing
            if "Dataset already exists" in str(e):
                pass
                # Skip table creation because it already exists
                # print("Table already exists, skipping creation.")
            else:
                # If the error is due to another reason, re-raise the exception
                raise

    def update_db(self, update_df: pl.DataFrame, merge_on: str = "block_number"):
        """
        `update_db` performs an "upsert" operation on the logs table.
        """

        # check if db exists. If it doesn't, then create it
        self.create_db(update_df)

        self.logs_tbl.compact_files()
        self.logs_tbl.to_lance().optimize.optimize_indices()
        self.logs_tbl.cleanup_old_versions(datetime.timedelta(seconds=60))

        # Perform a "upsert" operation
        self.logs_tbl.merge_insert(merge_on)   \
            .when_not_matched_insert_all() \
            .execute(update_df)

        # lance db cleanup
        # make table fragments compact
        # 1m target rows per file

        self.logs_tbl.cleanup_old_versions(
            older_than=datetime.timedelta(seconds=600), delete_unverified=True
        )
