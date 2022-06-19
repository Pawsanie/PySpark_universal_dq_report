from pyspark.sql import SparkSession
import pyspark.sql.functions as py_sql
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType
from datetime import date, timedelta, datetime
import argparse

"""
The script reads the dataset along the path and selects the columns 
in it received from the argument for the specified dates.
Then it saves the report to the specified path of HDFS.
"""


def get_report(interest_ids, interest_columns, dataset_name, path_to_dataset,
               interest_days, path_to_save_file, partition_type):
    """
    Spark Session
    --------------
    Look at schema...
    'interest_columns' does not need to be received as an argument.
    It can be filled in manually.
    The 'scheme' variable can also be customized too.
    The main thing is that the columns in the 'result_df' variable
    match the columns in the given 'scheme' variable.
    --------------
    This example is an elementary report which in theory,
    should create a DataFrame with many rows that meet the requirements of 3 filters:
    * A value in the 'identifier' column is in 'interest_ids' list.
    * A value in the 'response' column contains the text 'Success' or 'Not_full_data'.
    * A value in the 'test_column' column contains the text 'interest_data_01' or 'interest_data_02'.
    As a result, a '.csv' table with values from columns 'column_1', 'column_2' and 'column_3' will be saved on HDFS.
    """
    spark = SparkSession.builder \
        .appName("Universal_DQ_report") \
        .getOrCreate()

    schema = StructType([
        StructField("column_1", StringType(), True),
        StructField("column_2", StringType(), True),
        StructField("column_3", StringType(), True)
    ])
    result = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    for day in interest_days:
        partition_path = f"{path_to_dataset}{dataset_name}/{day}{partition_type}"
        interest_partition = spark.read.parquet(partition_path)
        trace_by_id = interest_partition.filter(
            py_sql.col("identifier").isin(interest_ids))

        trace_with_success = trace_by_id.filter(py_sql.col('response').like('%Success%') |
                                                py_sql.col('response').like('%Not_full_data%'))

        trace_with_interest_data = trace_with_success.filter(py_sql.col('test_column').like('%interest_data_01%') |
                                                             py_sql.col('test_column').like('%interest_data_02%'))

        result_df = trace_with_interest_data.select(interest_columns)  # == Schema StructType columns.
        result = result.union(result_df)

    result.coalesce(1).write.csv(path_to_save_file, header=True)
    result.show(10, False)


def partition_type_checking(partition_type) -> str:
    """
    Gets the word identifier and infers the type of the dataset.
    -------------------
    Word MUST be 'daily' or 'hourly'!
    """
    if partition_type == 'daily':
        partition_type = '/*.parquet'
    if partition_type == 'hourly':
        partition_type = '/*/*.parquet'
    return partition_type


def string_to_list_parser(str_arg) -> list[str]:
    """
    Parse the list of elements from one string:
    Get a string with the interesting data.
    Break it character by character into elements.
    Generate list of strings with interesting data.
    -------------------
    Symbol to split MUST be ','!
    """
    interest_data_list = []
    str_arg_element = str_arg.split(',')
    arg_elements_count = len(str_arg_element)
    for element in range(arg_elements_count):
        interest_data_list.append(str_arg_element[element])
    return interest_data_list


def list_of_days(date_from, date_to) -> list[str]:
    """
    Parses dates creating a list of the days needed for the report.
    Get date type as input.
    Generate list of strings with interesting dates parts of hdfs path.
    """
    interest_days = []
    days_count = (date_from - date_to).days
    days_count = int(days_count)
    if days_count < 0:
        days_count *= -1
    for occasion in range(days_count):
        day_data = date_from + timedelta(days=+occasion)
        interest_days.append(datetime.strftime(day_data, '%Y/%m/%d'))
    interest_days.append(datetime.strftime(date_to, '%Y/%m/%d'))
    return interest_days


def args_processing():
    """
    Parsing Scripts Arguments.
    Get strings as input and output.
    """
    args_parser = argparse.ArgumentParser(description='Check partitions on hdfs.')
    args_parser.add_argument('-id', '--list_of_ids', required=True, help='List of identifiers for the report.')
    args_parser.add_argument('-cn', '--list_of_columns', required=True, help='List of columns for the report.')
    args_parser.add_argument('-n', '--name_of_dataset', required=True, help='Dataset name for writing and reading.')
    args_parser.add_argument('-p', '--path_to_dataset', required=True, help='Dataset path for reading.')
    args_parser.add_argument('-t', '--type_of_dataset', required=True, help='Daily or hourly dataset type.')
    args_parser.add_argument('-df', '--date_from', required=True, help='Start date of the report.')
    args_parser.add_argument('-dt', '--date_to', required=False,
                             help='End date of the report.', default=str(date.today()))
    return args_parser.parse_args()


def run():
    """
    The root variable responsible for starting the rest.
    """
    args = args_processing()
    list_of_ids = args.list_of_ids
    list_of_columns = args.list_of_columns
    dataset_name = args.name_of_dataset
    path_to_dataset = args.path_to_dataset
    type_of_dataset = args.type_of_dataset
    date_from = datetime.strptime(args.date_from, '%Y-%m-%d').date()
    date_to = datetime.strptime(args.date_to, '%Y-%m-%d').date()

    interest_ids = string_to_list_parser(list_of_ids)
    interest_columns = string_to_list_parser(list_of_columns)
    interest_days = list_of_days(date_from, date_to)
    path_to_save_file = f"{dataset_name}_{str(date_from)}-{str(date_to)}{'.csv'}"
    partition_type = partition_type_checking(type_of_dataset)

    get_report(interest_ids, interest_columns, dataset_name, path_to_dataset,
               interest_days, path_to_save_file, partition_type)


if __name__ == '__main__':
    run()
