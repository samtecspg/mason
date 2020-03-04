./scripts/check

export CLI=./cli/__init__.py

#python3 $CLI operator
#python3 $CLI operator table
#python3 $CLI operator table list
python3 $CLI operator table list -p database_name:crawler-poc
#python3 $CLI operator table get -p database_name:crawler-poc
#python3 $CLI operator table get -p database_name:crawler-poc,table_name:catalog_poc_data
#python3 $CLI operator table get -c examples/parameters/table_get.yaml
#python3 $CLI operator table refresh -p database_name:crawler-poc,table_name:catalog_poc_data
#python3 $CLI operator table infer -p database_name:crawler-poc,storage_path:lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/,schedule_name:test_crawler
#python3 $CLI operator table infer -p database_name:crawler-poc,storage_path:lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/,schedule_name:test_crawler
