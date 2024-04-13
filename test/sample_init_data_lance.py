from degen_tracker.lance import LanceDBLogs


# initialize this dataclass, which will be used to build the logs database
lance_logs = LanceDBLogs()


# initial db sync. If the DB already exists, then this will be skipped.
lance_logs.initial_db_sync(full_sync=False)

# print('STARTING TO UPDATE DB CONTINUOUSLY')
# # update the logs database every 5 seconds.
# lance_logs.update_db(refresh_rate=5)
