from degen_tracker.lance import LanceDBLogs
import time

# starts a full historical sync. Currently slow because hypersync doesn't implement parallelization with current method being used. 
# Also lancedb has a bug that doesn't handle memory properly, so always ends up running out of memory to write files.

sart_time = time.time()
# initialize this dataclass, which will be used to build the logs database
lance_logs = LanceDBLogs()

# initial db sync. Set start block = 0 for full sync
lance_logs.db_sync(start_block=0, block_chunks=50_000)

print('time took to sync base erc20 logs:',
      time.time() - sart_time)
