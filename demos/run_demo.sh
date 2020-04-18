#!/bin/bash
set -e

if [ $1 == "1.02" ]
then
  commands=(
    "mason"
    "mason config"
    "mason config examples/configs"
    "mason config -s 1"
    "mason operator"
    "mason register examples/operators/table"
    "mason register examples/operators/job"
    "mason operator"
    "mason operator table get -p \"database_name:spg-mason-demo,table_name:part_data\""
    "mason operator table merge -p \"input_path:spg-mason-demo/part_data/,output_path:spg-mason-demo/part_data_merged/\""
    "mason operator table get -p \"database_name:spg-mason-demo,table_name:part_data_merged\""
  )
else
  commands=()
fi

cd ..
mkdir '.tmp'
export MASON_HOME='.tmp/'

for i in "${commands[@]}"
do
  # echo "Press [Enter] to continue demo:"
  # read -p "$i"
  eval "$i"
done

trap 'rm -rf .tmp' EXIT

