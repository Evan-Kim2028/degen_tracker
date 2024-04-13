from degen_tracker.metrics import Metrics
import time

metrics = Metrics()

while True:
    df = metrics.count_transfers()
    print(f'newest block: {df["block_number"].max()}')
    print(df.head(10).to_dicts())
    time.sleep(5)
