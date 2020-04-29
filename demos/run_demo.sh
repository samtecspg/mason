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
    "mason config -s 2"
    "mason register examples/operators/table"
    "mason register examples/operators/job"
    "mason operator"
    "mason operator table get -p \"database_name:spg-mason-demo,table_name:conflicting-parquet\""
    "mason config -s 0"
    "mason operator table infer -p \"database_name:spg-mason-demo,schedule_name:mason-demo-crawler-2,storage_path:spg-mason-demo/conflicting-parquet"
    "echo && cat query1.txt"
    "export JOB_ID=$(cat query1.txt | grep "job id" | tail -n 1 | cut -d= -f2)"
    "mason operator job get -p \"job_id:$JOB_ID\""
    "mason operator table delete -p \"database_name:spg-mason-demo,table_name:conflicting-parquet"
    "mason operator table infer -p \"database_name:spg-mason-demo,schedule_name:mason-demo-crawler-2,storage_path:spg-mason-demo/conflicting-parquet/file_1.parquet\""
    "echo \"wait a bit for table to be created by glue\""
    "mason config -s 1"
    "mason operator table query -p \"database_name:spg-mason-demo,query_string:SELECT * from \\\"file_1_parquet\\\" limit 5\" >> out.txt"
    "export JOB_ID=$(cat out.txt | grep "job id" | head -n 1 | cut -d= -f2)"
    "mason operator job get -p \"job_id:$JOB_ID\""
    "echo \"Wait a bit for job to run\""
    "mason operator job get -p \"job_id:$JOB_ID\""
    "mason operator table delete -p \"database_name:spg-mason-demo,table_name:file_1_parquet"
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


