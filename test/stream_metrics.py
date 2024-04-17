
import time

from degen_tracker.metrics import Metrics


metrics = Metrics()

while True:
    print('repeating loop')
    df = metrics.count_transfers()
    print(f'newest block: {df["block_number"].max()}')
    # convert output to dics and print output
    print(df.head(10).to_dicts())
    # wait 5 seconds before starting again
    time.sleep(5)
