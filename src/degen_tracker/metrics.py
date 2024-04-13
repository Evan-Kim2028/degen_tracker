import polars as pl
import lancedb
import time
from dataclasses import dataclass


@dataclass
class Metrics:

    def stream(self) -> pl.DataFrame:
        """
        Streams metrics from the database. Useful for frontend input.
        """

        # every 5 seconds, update group_by
        while True:
            print(self.count_transfers())
            time.sleep(5)

    def group_by_addresses(self) -> pl.DataFrame:
        """
        Groups metrics by addresses.

        df is the df loaded from the lancedb database
        """

        uri: str = "logs"
        self.db = lancedb.connect(uri)
        db: lancedb.DBConnection = lancedb.connect(uri)
        logs_tbl = db.open_table(uri)

        # convert dataset to table (full in memory)
        table = logs_tbl.to_pandas()
        pl_df = pl.from_pandas(table)

        return pl_df.sort(by='block_number', descending=True).filter(pl.col('value_transferred') != 0).group_by(
            'from').agg(pl.len().alias('address_swap_count')).sort(by='address_swap_count', descending=True)

    def count_transfers(self) -> pl.DataFrame:
        uri: str = "logs"
        self.db = lancedb.connect(uri)
        db: lancedb.DBConnection = lancedb.connect(uri)
        logs_tbl = db.open_table(uri)

        # convert dataset to table (full in memory)
        table = logs_tbl.to_pandas()
        pl_df = pl.from_pandas(table)

        return pl_df.sort(by='block_number', descending=True).group_by('block_number').agg(pl.len().alias('transfers_count')).sort(by='block_number', descending=True)
