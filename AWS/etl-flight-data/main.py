from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, mean, sum, count, concat_ws, udf
from pyspark.sql.window import Window
import os

bucket_name = "datalake-personal-projects"
project_name = "etl-flight-data"


spark = SparkSession.builder.appName("etl-flight-data").getOrCreate()

glueContext = GlueContext(spark)
logger = glueContext.get_logger()

#reading local file need "file:///"
# df = spark.read.option("header", True).csv("file:///datasets/flights.csv", sep=",", inferSchema=True)

#reading from s3
# df = spark.read.option("header", True).csv("s3a://datalake-personal-projects/etl-flight-data/flights.csv", sep=",", inferSchema=True)

# Read data from the data source
df_frame = glueContext.create_dynamic_frame.from_catalog(
 database="personal-projects",
 table_name="raw-flight-data"
)


df_frame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                              connection_options= {"paths": [f"s3://{bucket_name}/{project_name}/raw/flights.csv"]},
                                              format = "csv",
                                              format_options={
                                                  "withHeader": True                                              
                                                  })

#defining schema
mapping = [
    ("id", "int", "id", "int"),
    ("year", "int", "year", "int"),
    ("month", "int", "month", "int"),
    ("day", "int", "day", "int"),
    ("dep_time", "double", "dep_time", "double"),
    ("sched_dep_time", "int", "sched_dep_time", "int"),
    ("dep_delay", "double", "dep_delay", "double"),
    ("arr_time", "double", "arr_time", "double"),
    ("sched_arr_time", "int", "sched_arr_time", "int"),
    ("arr_delay", "double", "arr_delay", "double"),
    ("carrier", "string", "carrier", "string"),
    ("flight", "int", "flight", "int"),
    ("tailnum", "string", "tailnum", "string"),
    ("origin", "string", "origin", "string"),
    ("dest", "string", "dest", "string"),
    ("air_time", "double", "air_time", "double"),
    ("distance", "int", "distance", "int"),
    ("hour", "int", "hour", "int"),
    ("minute", "int", "minute", "int"),
    ("time_hour", "timestamp", "time_hour", "timestamp"),
    ("name", "string", "name", "string")
]

#forcing types in dynamicFrame
spec_resolveChoice = [(tuple[2], "cast:" + str(tuple[3])) for tuple in mapping]
df_frame = df_frame.resolveChoice(specs=spec_resolveChoice)

# # Apply mapping
# df_frame = df_frame.apply_mapping(mapping)

# Convert DynamicFrame to DataFrame for easier handling
df = df_frame.toDF()


# Handling with null values
col_with_nullValues=[]
for column in df.columns:
    n=df.filter(col(column).isNull()).count()
    if n !=0:
        logger.info(f"{column}: {n}")
        col_with_nullValues.append(column)
logger.info(f"Columns with null values: {col_with_nullValues}")

string_cols = [field.name for field in df.schema.fields if field.dataType.simpleString() == "string" and field.name in col_with_nullValues]
numeric_cols = [field.name for field in df.schema.fields if field.dataType.simpleString() in ("int", "double") and field.name in col_with_nullValues]

# Drop rows where any string column has null
df = df.dropna(subset=string_cols)

#computing means for each numeric column
means = df.select(
    *[mean(col(col_name)).alias(col_name) for col_name in numeric_cols]
).collect()[0].asDict()

# Fill missing values in numeric columns with means
df = df.fillna({**means})


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
)

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
)




# Convert DataFrames back to DynamicFrames and write to S3
dynamic_frame_carrier = DynamicFrame.fromDF(df_carrier_analysis, glueContext, "carrier_analysis")
dynamic_frame_route = DynamicFrame.fromDF(df_route_analysis, glueContext, "route_analysis")


glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_carrier,
    connection_type="s3",
    connection_options={"path": f"s3://{bucket_name}/{project_name}/curated/carrier_analysis/", 
                        "partitionKeys": ["year", "carrier"]},
    format="parquet"
)

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_route,
    connection_type="s3",
    connection_options={"path": f"s3://{bucket_name}/{project_name}/curated/route_analysis/", 
                        "partitionKeys": ["year", "route"]},
    format="parquet"
)