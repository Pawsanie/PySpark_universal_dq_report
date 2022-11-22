#!/bin/bash
# List or one id like string split by ',' without space.
id_list='1234561,123452,123453'
# Dataset`s name like string.
dataset_name='Name'
# Partition path on HDFS, like string.
partition_path='/example_warehouse/example_root/example_catalog/'
# Daily or hourly dataset type (daily/hourly):
type_of_dataset='daily'
# The date you plan to receive the report from.
date_from='YYYY-MM-DD'
# The date you plan to receive the report to. If not specified, it will be today.
date_to='YYYY-MM-DD'

# Start:
python3 universal_dq_report.py \
-id $id_list \
-n $dataset_name \
-p $partition_path \
-t $type_of_dataset \
-df $date_from \
-dt $date_to
