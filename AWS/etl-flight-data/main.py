from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, mean, sum, count, max, min, when, concat_ws
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

# load .env vars
env_file_exists = os.path.exists(".env")
if env_file_exists==True:
    load_dotenv(".env")
    aws_access_key_id = os.getenv("aws_access_key_id")
    aws_secret_access_key = os.getenv("aws_secret_access_key")
    conf = (
    SparkConf()
    .setAppName("Analyzing Flight Data") # replace with your desired name
    .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
    .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    .set("spark.sql.shuffle.partitions", "4") # default is 200 partitions which is too many for local
    .setMaster("local[*]") # replace the * with your desired number of cores. * for use all.
)
else:
    conf = (
    SparkConf()
    .setAppName("Analyzing Flight Data") # replace with your desired name
    .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
    .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.shuffle.partitions", "4") # default is 200 partitions which is too many for local
    .setMaster("local[*]") # replace the * with your desired number of cores. * for use all.
    )



spark = SparkSession.builder.config(conf=conf).getOrCreate()

#reading local file need "file:///"
# df = spark.read.option("header", True).csv("file:///datasets/flights.csv", sep=",", inferSchema=True)

#reading from s3
df = spark.read.option("header", True).csv("s3a://datalake-personal-projects/etl-flight-data/flights.csv", sep=",", inferSchema=True)


# Removing column with >90% null values
threshold_nulls = 0.9
row_count = df.count()

columns_to_keep = [
    col_name
    for col_name in df.columns
    if df.filter(col(col_name).isNotNull()).count()/row_count > threshold_nulls
]

# Drop columns with more than 90% null values
df = df.select(*columns_to_keep)

# Define window specifications
carrier_year_month_window = Window.partitionBy("year", "month", "carrier")
carrier_year_hour_window = Window.partitionBy("year", "hour", "carrier")
carrier_year_window = Window.partitionBy("year", "carrier")
route_year_month_window = Window.partitionBy("year", "month", "route")
route_year_hour_window = Window.partitionBy("year", "hour", "route")
route_year_window = Window.partitionBy("year", "route")


#Creating new columns
df = df.withColumn("total_delay", col("arr_delay") + col("dep_delay"))
df = df.withColumn("route", concat_ws("-", col("origin"), col("dest")))

##### WINDOW FUNCTION COLUMNS #######
##### CARRIER #######################################
#arrive delay
df = df.withColumn(
    "carrier_year_hour_avg_arr_delay", mean("arr_delay").over(carrier_year_hour_window)
    )
df = df.withColumn(
    "carrier_year_month_avg_arr_delay", mean("arr_delay").over(carrier_year_month_window)
    )
df = df.withColumn(
    "carrier_year_avg_arr_delay", mean("arr_delay").over(carrier_year_window)
    )
#departure delay
df = df.withColumn(
    "carrier_year_hour_avg_dep_delay", mean("dep_delay").over(carrier_year_hour_window)
    )
df = df.withColumn(
    "carrier_year_month_avg_dep_delay", mean("dep_delay").over(carrier_year_month_window)
    )
df = df.withColumn(
    "carrier_year_avg_dep_delay", mean("dep_delay").over(carrier_year_window)
    )
# total delay
df = df.withColumn(
    "carrier_year_hour_avg_total_delay", mean("total_delay").over(carrier_year_hour_window)
    )
df = df.withColumn(
    "carrier_year_month_avg_total_delay", mean("total_delay").over(carrier_year_month_window)
    )
df = df.withColumn(
    "carrier_year_avg_total_delay", mean("total_delay").over(carrier_year_window)
    )
#num flights
df = df.withColumn(
    "carrier_year_hour_total_flights", count("flight").over(carrier_year_hour_window)
    )
df = df.withColumn(
    "carrier_year_month_total_flights", count("flight").over(carrier_year_month_window)
    )
df = df.withColumn(
    "carrier_year_total_flights", count("flight").over(carrier_year_window)
    )

##### ROUTE ##################################
#arrive delay
df = df.withColumn(
    "route_year_hour_avg_arr_delay", mean("arr_delay").over(route_year_hour_window)
    )
df = df.withColumn(
    "route_year_month_avg_arr_delay", mean("arr_delay").over(route_year_month_window)
    )
df = df.withColumn(
    "route_year_avg_arr_delay", mean("arr_delay").over(route_year_window)
    )
#departure delay
df = df.withColumn(
    "route_year_hour_avg_dep_delay", mean("dep_delay").over(route_year_hour_window)
    )
df = df.withColumn(
    "route_year_month_avg_dep_delay", mean("dep_delay").over(route_year_month_window)
    )
df = df.withColumn(
    "route_year_avg_dep_delay", mean("dep_delay").over(route_year_window)
    )
# total delay
df = df.withColumn(
    "route_year_hour_avg_total_delay", mean("total_delay").over(route_year_hour_window)
    )
df = df.withColumn(
    "route_year_month_avg_total_delay", mean("total_delay").over(route_year_month_window)
    )
df = df.withColumn(
    "route_year_avg_total_delay", mean("total_delay").over(route_year_window)
    )
#num flights
df = df.withColumn(
    "route_year_hour_total_flights", count("flight").over(route_year_hour_window)
    )
df = df.withColumn(
    "route_year_month_total_flights", count("flight").over(route_year_month_window)
    )
df = df.withColumn(
    "route_year_total_flights", count("flight").over(route_year_window)
    )


#Creating analysis tables
df_carrier_analysis = df.groupBy("year","month","hour","carrier").agg(
    #window functions columns
    #arr delay
    mean("arr_delay").alias("carrier_year_month_hour_avg_arr_delay"),
    mean("carrier_year_month_avg_arr_delay").alias("carrier_year_month_avg_arr_delay"),
    mean("carrier_year_hour_avg_arr_delay").alias("carrier_year_hour_avg_arr_delay"),
    #dep delay
    mean("dep_delay").alias("carrier_year_month_hour_avg_dep_delay"),
    mean("carrier_year_month_avg_dep_delay").alias("carrier_year_month_avg_dep_delay"),
    mean("carrier_year_hour_avg_dep_delay").alias("carrier_year_hour_avg_dep_delay"),
    #total delay
    mean("total_delay").alias("carrier_year_month_hour_avg_total_delay"),
    mean("carrier_year_month_avg_total_delay").alias("carrier_year_month_avg_total_delay"),
    mean("carrier_year_hour_avg_total_delay").alias("carrier_year_hour_avg_total_delay"),
    #totl flights
    count("flight").alias("carrier_year_month_hour_total_flights"),
    mean("carrier_year_month_total_flights").alias("carrier_year_month_total_flights"),
    mean("carrier_year_hour_total_flights").alias("carrier_year_hour_total_flights"),
).show()

df_route_analysis = df.groupBy("year","month","hour","route").agg(
    #window functions columns
    #arr delay
    mean("arr_delay").alias("route_year_month_hour_avg_arr_delay"),
    mean("route_year_month_avg_arr_delay").alias("route_year_month_avg_arr_delay"),
    mean("route_year_hour_avg_arr_delay").alias("route_year_hour_avg_arr_delay"),
    #dep delay
    mean("dep_delay").alias("route_year_month_hour_avg_dep_delay"),
    mean("route_year_month_avg_dep_delay").alias("route_year_month_avg_dep_delay"),
    mean("route_year_hour_avg_dep_delay").alias("route_year_hour_avg_dep_delay"),
    #total delay
    mean("total_delay").alias("route_year_month_hour_avg_total_delay"),
    mean("route_year_month_avg_total_delay").alias("route_year_month_avg_total_delay"),
    mean("route_year_hour_avg_total_delay").alias("route_year_hour_avg_total_delay"),
    #totl flights
    count("flight").alias("route_year_month_hour_total_flights"),
    mean("route_year_month_total_flights").alias("route_year_month_total_flights"),
    mean("route_year_hour_total_flights").alias("route_year_hour_total_flights"),
).show()


