from argparse import ArgumentParser
from datetime import datetime, timedelta
from os.path import isfile

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from pyspark.sql.functions import col, sum

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


def aggregate_daily(sp:SparkSession, schema: StructType, input_path: str, output_path: str, name: str):
    df = sp.read.csv(path=input_path + name, schema=schema, header=False)
    df.groupBy("email")\
        .pivot("crud")\
        .count()\
        .select(
            col("email"),
            col("CREATE").alias("create_count"),
            col("READ").alias("read_count"),
            col("UPDATE").alias("update_count"),
            col("DELETE").alias("delete_count"),
        )\
        .coalesce(1).write.csv(path=output_path+name, header=True)


for name in last_week_daily_files:
    if not isfile(args.daily_path + name):
        if isfile(args.input_path + name):
            aggregate_daily(spark, raw_data_schema, args.input_path, args.daily_path, name)
        else:
            raise FileNotFoundError(f"No file named {name} in input directory {args.input_path}")

last_week_daily_path = [args.daily_path + name for name in last_week_daily_files]

df = spark.read.csv(path=last_week_daily_path, schema=daily_data_schema, header=True)
df.groupBy("email")\
    .agg(
        sum(col("create_count")).alias("create_count"),
        sum(col("read_count")).alias("read_count"),
        sum(col("update_count")).alias("update_count"),
        sum(col("delete_count")).alias("delete_count"),
    )\
    .coalesce(1).write.csv(path=args.output_path + start_date.strftime("%Y-%m-%d.csv"), header=True)
