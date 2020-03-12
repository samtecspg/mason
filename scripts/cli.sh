#!/bin/sh
set -e

# Reset everything to 0
# ./scripts/install.sh
# rm -rf ~/.mason/

# config examples
# mason config -l debug
# mason config examples/config/bad_config.yaml
# mason config examples/config/config_example.yaml
# mason config
# mason register
# mason register examples/operators/table
# mason run

# register exampels
# mason register examples/operators/table/list/  # TODO: PLEASE FIX THIS CASE
# mason register examples/operators/table/

# operator examples
# mason operator
# mason operator -l debug
# mason operator table
# mason operator table get
# mason operator table get -p datebase_name:crawler-poc,bad
# mason operator table get -p bad
# mason operator table get -p database_name:crawler-poc,table_name:catalog_poc_data

# list examples
# mason operator table list
# mason operator table list -p database_name:crawler-poc

# refresh examples
# mason operator table refresh -p database_name:crawler-poc,table_name:catalog_poc_data

# infer examples
# mason operator table infer -p schedule_name:test_crawler,database_name:crawler-poc,storage_path:lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/
# mason operator table infer -p schedule_name:crawler-shipment,database_name:crawler-shipment,storage_path:lake-working-copy-feb-20-2020/logistics-bi-data-publisher/prod/shipment/

