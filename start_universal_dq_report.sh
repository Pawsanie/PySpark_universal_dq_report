#!/bin/bash
# List or one id like string split by ',' without space.
id_list='1234561,123452,123453'
# List or one column like string split by ',' without space.
column_list='column_1,column_2,column_3'
# Dataset`s name like string.
dataset_name='Name'
# Partition path on HDFS, like string.
partition_path='/exemple_warhaus/exemple_root/exemple_catalog/'
# Daily or hourly dataset type (daily/hourly):
type_of_dataset='daily'
# The date you plan to receive the report from.
date_from='YYYY-MM-DD'
# The date you plan to receive the report to. If not specified, it will be today.
date_to='YYYY-MM-DD'

# Start:
python3 universal_dq_report.py \
-id $id_list \
-cn $column_list \
-n $dataset_name \
-p $partition_path \
-t $type_of_dataset \
-df $date_from \
-dt $date_to
