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
    "mason config -s 1"
    "mason register examples/operators/table"
    "mason register examples/operators/job"
    "mason operator"
    "mason operator table list -p \"database_name:crawler-poc\""
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


