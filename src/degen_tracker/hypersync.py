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

    def get_erc20_df(self, start_block: int, end_block: int) -> pl.DataFrame:
        """
        sync_erc20s() is a synchronous wrapper function around the asynchronous fetch_erc20s() function.

        sync is a boolean value that determines whether to sync all erc20 transfers from block 0 or from the latest block. It is False by default.
        If false, it will simply sync to the block head.

        block_start is the block number to start syncing from
        block_end is the block number to end syncing at

        Returns:
            dict: A dictionary containing the following keys:
                {
                    "tx_data": tx_data,
                    "decoded_log_data": decoded_log_data,
                    "log_data": log_data,
                    "block_data": block_data
                }
        """
        data_dict = asyncio.run(self.fetch_erc20s(start_block=start_block,
                                                  end_block=end_block))
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

    async def fetch_erc20s(self, start_block: int, end_block: int) -> dict[str]:
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

        query = hypersync.Query(
            to_block=end_block,
            from_block=start_block,
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

        # DATA ORGANIZTION
        tx_data = []
        decoded_log_data = []
        log_data = []
        block_data = []

        print(f"""starting to run query from block {start_block} to block {
              end_block}. {end_block - start_block} blocks to fetch.""")
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
            if end_block == res.next_block:
                # Exit the loop if the end of the period (or the blockchain's current height) is reached.
                break

            # Update the query to fetch the next set of data starting from the next block.
            query.from_block = res.next_block
            # Log progress. # archive_height is the newest height of the block
            # print(f"archive height: {res.archive_height}")
            # print(f"from block: {query.from_block}")
            # print(f"up to block: {end_block}")

        return {
            "tx_data": tx_data,
            "decoded_log_data": decoded_log_data,
            "log_data": log_data,
            "block_data": block_data
        }
