import hypersync
from hypersync import LogField, DataType, ColumnMapping, TransactionField
import asyncio
import time

# Code adapted from this example - https://github.com/enviodev/hypersync-client-python/blob/main/examples/top-usdt.py
# Currently does not work as the query crashes and can't handle null values. This is a Hypersync issue with parquet configuration.


async def collect_events():
    client = hypersync.HypersyncClient("https://base.hypersync.xyz")

    height = await client.get_height()

    query = hypersync.Query(
        # Full sync
        from_block=2179494,
        logs=[hypersync.LogSelection(
            # We want All ERC20 transfers so no address filter and only a filter for the first topic
            topics=[
                ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]]
        )],
        field_selection=hypersync.FieldSelection(
            log=[el.value for el in LogField],
            transaction=[TransactionField.BLOCK_NUMBER,
                         TransactionField.TRANSACTION_INDEX,
                         TransactionField.HASH,
                         TransactionField.FROM,
                         TransactionField.TO
                         ],
            block=[el.value for el in hypersync.BlockField]
        )
    )

    config = hypersync.ParquetConfig(
        path="data",
        hex_output=True,
        # column_mapping=ColumnMapping(
        #     # map value columns to float so we can do calculations with them
        #     decoded_log={
        #         "value": DataType.FLOAT64,
        #     },
        #     transaction={
        #         TransactionField.GAS_USED: DataType.FLOAT64,
        #     },
        # ),
        # give event signature so client can decode logs into decoded_logs.parquet file
        event_signature="Transfer(address indexed from, address indexed to, uint256 value)",
        batch_size=10_000,
        concurrency=10,
        retry=True

    )

    await client.create_parquet_folder(query, config)

start_time = time.time()
asyncio.run(collect_events())
end_time = time.time()

print(f"Time taken: {end_time - start_time}")   # 10gb, 5:13PM


# def analyze_events():
#     # read raw logs
#     logs = polars.read_parquet(
#         "data/logs.parquet",
#     )

#     # read transactions
#     transactions = polars.read_parquet(
#         "data/transactions.parquet",
#     )

#     # read decoded logs and join(stack) the rows with raw logs.
#     # then join transactions using the tx hash column from raw logs table.
#     data = polars.read_parquet(
#         "data/decoded_logs.parquet"
#     ).hstack(logs).join(
#         other=transactions,
#         left_on=polars.col("transaction_hash"),
#         right_on=polars.col("hash"),
#     ).group_by(
#         polars.col("from")
#     ).agg(
#         polars.col("value").sum().alias("total_value_sent"),
#         polars.col("gas_used").sum().alias("total_gas_used"),
#     ).sort(
#         polars.col("total_value_sent"),
#         descending=True
#     ).limit(10)

#     polars.Config.set_ascii_tables()
#     polars.Config.set_tbl_width_chars(100)
#     polars.Config.set_fmt_str_lengths(50)

#     print(data)


# analyze_events()
