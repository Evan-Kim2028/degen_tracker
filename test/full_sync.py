from degen_tracker.lance import LanceDBLogs
import time


sart_time = time.time()
# initialize this dataclass, which will be used to build the logs database
lance_logs = LanceDBLogs()

# initial db sync. If the DB already exists, then this will be skipped.
lance_logs.initial_db_sync(full_sync=True)

print('time took to sync base erc20 logs:',
      time.time() - sart_time)    # 1.6gb, 4:54PM


# print('STARTING TO UPDATE DB CONTINUOUSLY')
# # update the logs database every 5 seconds.
# lance_logs.update_db(refresh_rate=5)
