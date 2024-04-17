from degen_tracker.lance import LanceDBLogs
import time

# starts a full historical sync. Currently slow because hypersync doesn't implement parallelization with current method being used.
# Also lancedb has a bug that doesn't handle memory properly, so always ends up running out of memory to write files.

sart_time = time.time()
# initialize this dataclass, which will be used to build the logs database
lance_logs = LanceDBLogs()

# initial db sync. Set start block = 2000000 for full sync. Setting at 0 crashes because not all block chunk ranges had a tx/log inside of it at the genesis of the chain.
lance_logs.db_sync(start_block=2000000, block_chunks=10000)

print('time took to sync base erc20 logs:',
      time.time() - sart_time)
