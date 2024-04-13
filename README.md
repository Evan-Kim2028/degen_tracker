# degen-tracker

degen_tracker helps users make more informed decisions when trading tokens on base. 


### Setup
`degen_tracker` utilizes Hypersync as the execution layer storage engine to retrieve erc20 transfers in the logs and decode them.

`lancedb` is used to continuously update the database with new erc20 transfers. `Lance` file format is used because it's a columnar
type file that allows for mutability, unlike parquet files. 