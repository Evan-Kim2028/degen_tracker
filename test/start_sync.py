from degen_tracker.lance import LanceDBLogs
import time


sart_time = time.time()
# initialize this dataclass, which will be used to build the logs database
lance_logs = LanceDBLogs()

# initial db sync. Set start block = 0 for full sync
lance_logs.db_sync(start_block=0, block_chunks=50_000)

print('time took to sync base erc20 logs:',
      time.time() - sart_time)
