from argparse import ArgumentParser
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

parser = ArgumentParser(description="date of start | input path| output path| daily path")

parser.add_argument("--execution_date", help="Date of start weekly aggregate")
parser.add_argument("--input_path", help="Folder with .csv files")
parser.add_argument("--output_path", help="Folder to save .csv files that contain weekly aggregate")
parser.add_argument("--daily_path", help="Folder to save .csv files that contain daily aggregate")

args = parser.parse_args()

start_date = datetime.strptime(args.execution_date, "%Y-%m-%d")
last_week_daily_files = [(start_date - timedelta(days=i)).strftime("%Y-%m-%d.csv") for i in range(1, 8)]

spark = SparkSession.builder.appName("Weekly user CRUD action counter").getOrCreate()

raw_data_schema = StructType([
    StructField("email", StringType()),
    StructField("crud", StringType()),
    StructField("timestamp", TimestampType()),
])

daily_data_schema = StructType([
    StructField("email", StringType()),
    StructField("create_count", LongType()),
    StructField("read_count", LongType()),
    StructField("update_count", LongType()),
    StructField("delete_count", LongType()),
])