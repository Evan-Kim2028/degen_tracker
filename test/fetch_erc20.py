from degen_tracker.hypersync import Hypersync

client = Hypersync()


# get the erc20s. Overall workflow
data_dict = client.sync_erc20s()

print(data_dict['log_data'][0])
print(data_dict['log_data'][0].body)
print(data_dict['log_data'][0].indexed)

print(data_dict['tx_data'][0])

print('done')
