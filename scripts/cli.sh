#!/bin/sh
set -e

./scripts/install.sh
rm -rf ~/.mason/
mason config -l debug
# mason config examples/config/bad_config.yaml
mason config examples/config/config_example.yaml
# mason config
# mason register
#mason register examples/operators/table
# mason run
# mason register examples/operators/table/list/  # TODO: PLEASE FIX THIS CASE
 mason register examples/operators/table/
# mason operator
# mason operator -l debug
# mason operator table
# mason operator table get
# mason operator table get -p datebase_name:crawler-poc,bad
# mason operator table get -p bad
# mason operator table get -p database_name:crawler-poc,table_name:catalog_poc_data
# mason operator table list
# mason operator table list -p database_name:crawler-poc
# mason operator table refresh -p database_name:crawler-poc,table_name:catalog_poc_data
 mason operator table infer -p schedule_name:crawler-shipment,database_name:crawler-shipment,storage_path:lake-working-copy-feb-20-2020/logistics-bi-data-publisher/prod/shipment/

