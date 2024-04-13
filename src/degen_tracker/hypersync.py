import asyncio
import hypersync
import os
import polars as pl
from dataclasses import dataclass, field
from typing import List
from hypersync import TransactionField, HypersyncClient, LogField


@dataclass
class Hypersync:
    client: HypersyncClient = field(
        default_factory=lambda: HypersyncClient("https://base.hypersync.xyz"))
    transactions: List[hypersync.TransactionField] = field(
        default_factory=list)
    blocks: List[hypersync.BlockField] = field(default_factory=list)

    def convert_hex_to_float(self, hex: str) -> float:
        """
        Converts hexadecimal values in a transaction dictionary to integers, skipping specific keys.

        Args:
        transaction (dict): A dictionary containing transaction data, where some values are hexadecimals.

        Returns:
        dict: A new dictionary with hexadecimals converted to integers, excluding specified keys.
        """
        # Only convert hex strings; leave other values as is
        if isinstance(hex, str) and hex.startswith("0x"):
            # Convert hex string to float
            return float(int(hex, 16))

    async def get_block_height(self) -> int:
        """
        Get the current block height from the blockchain.

        Returns:
            int: The current block height.
        """
        return await self.client.get_height()

    def get_erc20_df(self, sync_all: bool, block_num_range: int = 5000) -> pl.DataFrame:
        """
        sync_erc20s() is a synchronous wrapper function around the asynchronous fetch_erc20s() function.

        sync is a boolean value that determines whether to sync all erc20 transfers from block 0 or from the latest block. It is False by default.
        If false, it will simply sync to the block head.

        Returns:
            dict: A dictionary containing the following keys:
                {
                    "tx_data": tx_data,
                    "decoded_log_data": decoded_log_data,
                    "log_data": log_data,
                    "block_data": block_data
                }
        """
        height = asyncio.run(self.get_block_height())

        match sync_all:
            case True:
                data_dict = asyncio.run(self.fetch_erc20s(sync_all=True, block_number=height,
                                                          block_num_range=block_num_range))
            case False:
                data_dict = asyncio.run(self.fetch_erc20s(sync_all=False, block_number=height,
                                                          block_num_range=block_num_range))

        # data transformations
        joined_logs = []
        # check if data_dict["log_data"] is None. If it is, then pass
        if data_dict["log_data"] is None:
            return pl.DataFrame()

        for i in range(len(data_dict["log_data"])):
            log = {
                "block_number": data_dict["log_data"][i].block_number,
                "tx_hash": data_dict["log_data"][i].transaction_hash,
                "indexed": data_dict["decoded_log_data"][i].indexed,
                # contains value transferred. Make float to avoid overflow errors
                "body": float(data_dict["decoded_log_data"][i].body[0]),
            }
            joined_logs.append(log)

        tx_data = []

        for tx in data_dict["tx_data"]:
            tx_data.append(
                {
                    "tx_hash": tx.hash,
                    "block_number": tx.block_number,
                    "from": tx.from_,
                    "to": tx.to,
                }
            )

        logs_df = pl.from_dicts(joined_logs)
        tx_df = pl.from_dicts(tx_data)

        return logs_df.join(tx_df, on=["tx_hash", 'block_number'], how="left").rename({'body': 'value_transferred'})

    async def fetch_erc20s(self, sync_all: bool, block_number: int, block_num_range: int) -> dict[str]:
        """
            - TODO get latest block header from lance database and update

            Returns:
                dict: A dictionary containing the following keys:
                    {
                        "tx_data": tx_data,
                        "decoded_log_data": decoded_log_data,
                        "log_data": log_data,
                        "block_data": block_data
                    }
            """

        match sync_all:
            case True:
                print('full sync!')
                query = hypersync.Query(
                    # Full sync
                    from_block=0,
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
            case False:
                query = hypersync.Query(
                    # partial sync
                    # to_block=block_number,
                    from_block=block_number - block_num_range,
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

        print("Running the query...")

        # DATA ORGANIZTION
        tx_data = []
        decoded_log_data = []
        log_data = []
        block_data = []

        # While loop for pagination
        while True:
            res = await self.client.send_req(query)

            # Determine the directory containing the current script
            base_dir = os.path.dirname(__file__)

            # Build the path to the JSON file
            file_path = os.path.join(base_dir, 'abis', 'erc20.json')

            # Now you can safely open the file with the path
            with open(file_path, 'r') as json_file:
                abi = json_file.read()

            # Map of contract_address -> abi
            abis = {}

            for log in res.data.logs:
                abis[log.address] = abi

            # Create a decoder with our mapping
            decoder = hypersync.Decoder(abis)

            # Decode the log on a background thread so we don't block the event loop.
            # Can also use decoder.decode_logs_sync if it is more convenient.
            decoded_logs = await decoder.decode_logs(res.data.logs)

            # Accumulate data from response
            tx_data.extend(res.data.transactions)
            decoded_log_data.extend(decoded_logs)
            log_data.extend(res.data.logs)
            block_data.extend(res.data.blocks)

            # Check if the fetched data has reached the current archive height or next block.
            if res.archive_height < res.next_block:
                # Exit the loop if the end of the period (or the blockchain's current height) is reached.
                break

            # Update the query to fetch the next set of data starting from the next block.
            query.from_block = res.next_block
            print(f"Scanned up to block {query.from_block}")  # Log progress.

        return {
            "tx_data": tx_data,
            "decoded_log_data": decoded_log_data,
            "log_data": log_data,
            "block_data": block_data
        }
