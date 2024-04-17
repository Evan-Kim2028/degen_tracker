from degen_tracker.lance import LanceDBLogs
import time

# run this to start a partial sync from block 13180000


# initialize this dataclass, which will be used to build the logs database
lance_logs = LanceDBLogs()

# update this number with the latest block here https://basescan.org/ minus 1000
start_block: int = 13180000
lance_logs.db_sync(start_block=start_block, end_block=None, block_chunks=50)
