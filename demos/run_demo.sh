#!/bin/bash
set -e

rm -rf '.tmp/'
mkdir '.tmp'
export MASON_HOME='.tmp/'

if [ $1 == "1.02" ]
then
  commands=(
    "mason"
    "mason config"
    "mason config examples/configs/"
    "mason config -s 0"
    "mason operator"
    "mason register examples/operators/table"
    "mason register examples/operators/job"
    "mason operator"
    "mason operator table get -p \"database_name:spg-mason-demo,table_name:part_data\""
    "mason operator table merge -p \"input_path:spg-mason-demo/part_data/,output_path:spg-mason-demo/part_data_merged/\""
    "mason operator table get -p \"database_name:spg-mason-demo,table_name:part_data_csv\""
    "mason operator table merge -p \"input_path:spg-mason-demo/part_data_csv/,output_path:spg-mason-demo/part_data_csv_merged/\""
    "mason operator table get -p \"database_name:spg-mason-demo,table_name:part_data_json\""
    "mason operator table merge -p \"input_path:spg-mason-demo/part_data_json/,output_path:spg-mason-demo/part_data_json_merged/\""
  )
elif [ $1 == "1.03" ]
then
  commands=(
    "mason config examples/configs/"
    "mason register examples/operators/"
    "mason operator"
    "mason config -s 0"
    "mason operator table get -p \"database_name:spg-mason-demo,table_name:conflicting-parquet\""
    "mason config -s 1"
    "mason operator table infer -p \"database_name:spg-mason-demo,schedule_name:mason-demo-crawler,storage_path:spg-mason-demo/conflicting-parquet\""
    "echo \"Wait a bit for glue crawler to run\""
    "mason operator table query -p \"database_name:spg-mason-demo,query_string:SELECT * from \\\"conflicting_parquet\\\" limit 5\" >> query.txt"
    "echo \"Wait a bit for job to run\" & cat query.txt"
    "export JOB_ID=\"\$(cat query.txt | grep \"job id\" | tail -n 1 | cut -d= -f2 | sed -e 's/\"$//')\""
    "echo \$JOB_ID"
    "mason operator job get -p \"job_id:\$JOB_ID\""
    "mason operator table delete -p \"database_name:spg-mason-demo,table_name:conflicting_parquet\""
    "mason operator schedule delete -p \"schedule_name:mason-demo-crawler\""
    "mason operator table infer -p \"database_name:spg-mason-demo,schedule_name:mason-demo-crawler-2,storage_path:spg-mason-demo/conflicting-parquet/file_1.parquet\""
    "echo \"wait a bit for table to be created by glue\""
    "mason operator table query -p \"database_name:spg-mason-demo,query_string:SELECT * from \\\"file_1_parquet\\\" limit 5\" >> query.txt"
    "export JOB_ID=\"\$(cat query.txt | grep \"job id\" | tail -n 1 | cut -d= -f2 | sed -e 's/\"$//')\""
    "echo \$JOB_ID"
    "mason operator job get -p \"job_id:\$JOB_ID\""
    "echo \"Wait a bit for job to run\""
    "mason operator job get -p \"job_id:\$JOB_ID\""
    "mason operator table delete -p \"database_name:spg-mason-demo,table_name:file_1_parquet\""
    "mason operator schedule delete -p \"schedule_name:mason-demo-crawler-2\""
  )
elif [ $1 == "1.03a" ]
then
  commands=(
    "mason config examples/configs/"
    "mason register examples/operators/"
    "mason operator"
    "mason config -s 1"
    "mason operator table delete -p \"database_name:spg-mason-demo,table_name:conflicting_parquet\""
    "mason operator schedule delete -p \"schedule_name:mason-demo-crawler\""
    "mason operator table delete -p \"database_name:spg-mason-demo,table_name:file_1_parquet\""
    "mason operator schedule delete -p \"schedule_name:mason-demo-crawler-2\""
  )
elif [ $1 == "1.04" ]
then
  commands=(
      "mason config mason/examples/configs/ -l trace"
      "mason workflow"
      "mason workflow register mason/examples/workflows/"
      "mason workflow"
      "mason workflow table infer"
      "mason workflow table infer -f mason/examples/parameters/bad_file.yaml"
      "mason workflow table infer -f mason/examples/parameters/workflow_table_infer.yaml"
      "mason operator"
      "mason register mason/examples/operators/"
      "mason workflow table infer -f mason/examples/parameters/workflow_table_infer.yaml"
      "mason workflow table infer -f mason/examples/parameters/bad_workflow_table_infer.yaml"
      "mason workflow table infer -f mason/examples/parameters/workflow_table_infer.yaml -d"
      "mason workflow table infer -f mason/examples/parameters/workflow_table_infer.yaml -d -r"
      "mason config -s 3"
      "mason operator table"
      "mason operator table infer -p database_name:bad_database,storage_path:bogus"
      "mason operator table infer -p database_name:bad_database,storage_path:spg-mason-demo/part_data_parts -l trace"
      "mason operator table infer -p database_name:crawler-poc,storage_path:spg-mason-demo/part_data_parts -l trace >> infer.txt"
      "cat infer.txt"
      "export JOB_ID=\"\$(cat infer.txt | grep \"job id\" | tail -n 1 | cut -d= -f2 | sed -e 's/\"$//')\""
      "echo \$JOB_ID"
      "mason config -s 1"
      "mason operator job get -p \"job_id:\$JOB_ID\" -l trace"
      "mason operator table query -p \"database_name:crawler-poc,query_string:SELECT * from \\\"part_data_parts\\\" limit 5\" >> query.txt"
      "cat query.txt"
      "export JOB_ID=\"\$(cat query.txt | grep \"job id\" | tail -n 1 | cut -d= -f2 | sed -e 's/\"$//')\""
      "echo \$JOB_ID"
      "mason operator job get -p \"job_id:\$JOB_ID\""
  )
else
  commands=()
fi


for i in "${commands[@]}"
do
  echo "Press [Enter] to continue demo:"
  read -p ">> $i"
  eval "$i"
done


