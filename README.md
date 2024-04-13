# degen-tracker

degen_tracker helps users make more informed decisions when trading tokens on base. 

`degen_tracker` utilizes Hypersync as the execution layer storage engine to retrieve erc20 transfers in the logs and decode them.

`lancedb` is used to continuously update the database with new erc20 transfers. `Lance` file format is used because it's a columnar
type file that allows for mutability, unlike parquet files. 

### Getting Started
1. This repository uses rye to manage dependencies and the virtual environment. To install, refer to this link for instructions [here](https://rye-up.com/guide/installation/). 
2. Once rye is installed, run `rye sync` to install dependencies and setup the virtual environment, which has a default name of `.venv`. 
3. Activate the virtual environment with the command `source .venv/bin/activate`.

### Running the code
run `sample_stream_db.py` to get started streaming queried logs data into a database. Then run `stream_metrics.py` to stream a counting metric in real time.

