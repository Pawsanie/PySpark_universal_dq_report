from pyspark.sql import SparkSession
import pyspark.sql.functions as py_sql
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType
from datetime import date, timedelta, datetime
import argparse

"""
The script reads the dataset along the path and selects the columns 
in it received from the argument for the specified dates.
Then it saves the report to the specified path of HDFS.
"""


def get_report(*, interest_ids: list, dataset_name: str, path_to_dataset: str,
               interest_days: list, path_to_save_file: str, partition_type: str):
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
    * A value in the 'response' column contains the text 'Failure'.

    As a result, a '.csv' table with values from columns 'identifier',
    'column_1', 'column_2' and 'column_3' will be saved on HDFS.
    * Where identifier contains id.
    * Where column_1_all contains count of all results.
    * Where column_2_ok_more_3sec contains count of trace_with_success when the latency is more 3 seconds.
    * Where column_3_fail_low_3sec contains count of trace_with_success when the latency is less 3 seconds.
    """
    spark = SparkSession.builder \
        .appName("Universal_DQ_report") \
        .getOrCreate()

    result_dict = {}
    for identifier in interest_ids:  # Select data for id.
        schema = StructType([
            StructField("column_1_all", IntegerType(), True),
            StructField("column_2_ok_more_3sec", IntegerType(), True),
            StructField("column_3_fail_low_3sec", IntegerType(), True)])
        result_for_identifier = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        for day in interest_days:  # Select data for day.
            # "path_to_dataset", "dataset_name", "partition_type" in "partition_path" below can be hardcoded.
            # Look at the "args_processing" function for more information.
            partition_path = f"{path_to_dataset}/{dataset_name}/{day}{partition_type}"
            interest_partition = spark.read.parquet(partition_path)

            trace_by_id = interest_partition.filter(
                py_sql.col("identifier").isin(identifier))

            trace_with_success = trace_by_id.filter(
                py_sql.col('response').like('%"Success"%') |
                py_sql.col('response').like('%"Not_full_data"%'))

            trace_with_failure = trace_by_id.filter(
                py_sql.col('response').like('%"Failure"%'))

            trace_all = trace_with_success.union(trace_with_failure)

            column_1 = trace_with_success.count() + trace_with_failure.count()
            column_2 = trace_all.filter(
                ((py_sql.col('response_time') / 1000) - (py_sql.col('request_time') / 1000)) > 3).count()
            column_3 = trace_with_failure.filter(
                (py_sql.col('response_time') - py_sql.col('request_time')) < 3).count()

            result_df = spark.createDataFrame(data=[(column_1, column_2, column_3)], schema=schema)
            result_for_identifier = result_for_identifier.union(result_df)
        result_dict.update({identifier: result_for_identifier})

    # Union all id`s report.
    schema_result = StructType([
        StructField("identifier", StringType(), True),
        StructField("column_1_all", IntegerType(), True),
        StructField("column_2_ok_more_3sec", IntegerType(), True),
        StructField("column_3_fail_low_3sec", IntegerType(), True)
    ])
    result = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_result)
    for identifier in result_dict:
        result_df = result_dict.get(identifier)
        column_1 = result_df.agg({'column_1_all': 'sum'}).collect()[0][0]
        column_2 = result_df.agg({'column_2_ok_more_3sec': 'sum'}).collect()[0][0]
        column_3 = result_df.agg({'column_3_fail_low_3sec': 'sum'}).collect()[0][0]
        result_df = spark.createDataFrame(data=[(identifier, column_1, column_2, column_3)], schema=schema_result)
        result = result.union(result_df)
    result.coalesce(1).write.csv(path_to_save_file, header=True)
    result.show(1000, False)


# If you hardcoded "partition_type" remove function below an "--type_of_dataset" argument.
def partition_type_checking(partition_type: str) -> str:
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


def string_to_list_parser(str_arg: str) -> list[str]:
    """
    Parse the list of elements from one string:
    Get a string with the interesting data.
    Break it character by character into elements.
    Generate list of strings with interesting data.
    -------------------
    Symbol to split MUST be ','!
    """
    interest_data_list = []
    str_arg_element: list[str] = str_arg.split(',')
    arg_elements_count = len(str_arg_element)
    for element in range(arg_elements_count):
        interest_data_list.append(str_arg_element[element])
    return interest_data_list


def list_of_days(date_from: date, date_to: date) -> list[str]:
    """
    Parses dates creating a list of the days needed for the report.
    Get date type as input.
    Generate list of strings with interesting dates parts of hdfs path.
    """
    interest_days = []
    days_count: int = (date_from - date_to).days
    if days_count < 0:
        days_count *= -1
    for occasion in range(days_count):
        day_data = date_from + timedelta(days=+occasion)
        interest_days.append(day_data.strftime('%Y/%m/%d'))
    interest_days.append(date_to.strftime('%Y/%m/%d'))
    return interest_days


def args_processing():
    """
    Parsing Scripts Arguments.
    Get strings as input and output.
    """
    args_parser = argparse.ArgumentParser(description='Check partitions on hdfs.')
    args_parser.add_argument('-id', '--list_of_ids', required=True, help='List of identifiers for the report.')
    args_parser.add_argument('-df', '--date_from', required=True, help='Start date of the report.')
    args_parser.add_argument('-dt', '--date_to', required=False,
                             help='End date of the report.', default=str(date.today()))
    args_parser.add_argument('-pts', '--path_to_save', required=False,
                             default='', help='Path to save csv report on hdfs.')
    # I don't use the 3 arguments below in production.
    # It can usually be hardcoded in a DQ report script "get_report" function`s "partition_path" variable...
    args_parser.add_argument('-n', '--name_of_dataset', required=True, help='Dataset name for writing and reading.')
    args_parser.add_argument('-p', '--path_to_dataset', required=True, help='Dataset path for reading.')
    args_parser.add_argument('-t', '--type_of_dataset', required=True, help='Daily or hourly dataset type.')

    return args_parser.parse_args()


def run():
    """
    The root function responsible for starting the rest.
    """
    args: argparse.Namespace = args_processing()
    list_of_ids: str = args.list_of_ids
    dataset_name: str = args.name_of_dataset
    path_to_dataset: str = args.path_to_dataset
    type_of_dataset: str = args.type_of_dataset
    date_from: date = datetime.strptime(args.date_from, '%Y-%m-%d').date()
    date_to: date = datetime.strptime(args.date_to, '%Y-%m-%d').date()
    path_to_save: str = args.path_to_save

    interest_ids: list[str] = string_to_list_parser(list_of_ids)
    interest_days: list[str] = list_of_days(date_from, date_to)
    path_to_save_file: str = f"{path_to_save}{dataset_name}_{str(date_from)}---{str(date_to)}"
    partition_type: str = partition_type_checking(type_of_dataset)

    get_report(interest_ids=interest_ids,
               dataset_name=dataset_name,
               path_to_dataset=path_to_dataset,
               interest_days=interest_days,
               path_to_save_file=path_to_save_file,
               partition_type=partition_type)


if __name__ == '__main__':
    run()
