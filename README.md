# PySpark universal dq report

## Disclaimer:
**Using** some or all of the elements of this code, **You** assume **responsibility for any consequences!**

## Description of the report:
The script reads the dataset along the path and selects the columns 
in it received from the argument for the specified dates.
Then it saves the report to the specified path of HDFS

This example is an elementary report which in theory, <br/>
should create a DataFrame with many rows that meet the requirements of 3 filters:
* A value in the 'identifier' column is in 'interest_ids' list.
* A value in the 'response' column contains the text 'Success' or 'Not_full_data'.
* A value in the 'test_column' column contains the text 'interest_data_01' or 'interest_data_02'.
As a result, a '.csv' table with values from columns 'column_1', 'column_2' and 'column_3' will be saved on HDFS.

For the practical result, it is required to substitute the real column names and data for filters into the get_report variable.

## Installing the Required Packages:
```bash
pip install pyspark
```
## Launch:
If Your OS has a bash shell the ETL pipeline can be started using the bash script:
```bash
./start_universal_dq_report.sh
```
The script contains an example of all the necessary arguments to run.<br/>
To launch the pipeline through this script, do not forget to make it executable.
```bash
chmod +x ./start_universal_dq_report.sh
```
The script can also be run directly with python.
```bash
spark-submit --queue uat --num-executors 5 --executor-cores 16 --executor-memory 15G --driver-memory 4G universal_dq_report.py \
-id '1234561,123452,123453' \
-cn 'column_1,column_2,column_3' \
-n 'Name' \
-p '/exemple_warhaus/exemple_root/exemple_catalog/' \
-t 'daily' \
-df 'YYYY-MM-DD' \
-dt 'YYYY-MM-DD' \
-pts ''
```
Where you can set or not set the following arguments as you wish for spark-submit:
* --queue
* --num-executors
* --executor-cores
* --executor-memory
* --driver-memory

About script arguments:
* -id - List or one id like string split by ',' without space.
* -cn - List or one column like string split by ',' without space.
* -n - Dataset`s name like string.
* -p - Partition path on HDFS, like string.
* -t - Daily or hourly dataset type (daily/hourly).
* -df - The date you plan to receive the report from (format YYYY-MM-DD).
* -dt - The date you plan to receive the report to. If not specified, it will be today (format YYYY-MM-DD).
* -pts - The path to safe csv report on HDFS. If not specified, it will be users home directory.

Thank you for showing interest in my work.