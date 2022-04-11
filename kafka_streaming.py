import time
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, TimestampType, LongType

def getSparkSessionInstance():
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .appName("Structured Streaming ") \
            .master("local[*]") \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

spark = getSparkSessionInstance()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)

kafkaBrokers = "192.168.0.10:9092"
kafkaTopic = "SensorReadings"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaBrokers) \
    .option("subscribe", kafkaTopic) \
    .option("startingOffsets", "earliest") \
    .load()
	
df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

value_schema = StructType([
        StructField("sensor",StringType(),True),
        StructField("machine",StringType(),True),
        StructField("units",StringType(),True),
        StructField("time", LongType(), True),
        StructField("value", DoubleType(), True)
])

df2 = df1.withColumn("jsonData", from_json(col("value"), value_schema)) \
        .withColumn("timestamp", (col("timestamp").cast("long")/1000).cast("timestamp")) \
        .select("key", "jsonData.*", "timestamp")

df2.createOrReplaceTempView("sensor_find")

streamingDataFrame = spark.sql('SELECT timestamp, concat(key, " (", units, ")") AS key, value FROM sensor_find')

outStream = streamingDataFrame. \
    groupBy("key"). \
    avg('value')

#outStream = streamingDataFrame. \
#    withWatermark("timestamp", "1 minute"). \
#    groupBy(window("timestamp", "10 minutes", "5 minutes"), "key"). \
#    groupBy("key"). \
#    avg('value')

outStream = outStream.withColumnRenamed("avg(value)", "value")

query = outStream.writeStream. \
    outputMode('complete'). \
    option("numRows", 1000). \
    option("truncate", "false"). \
    format("console"). \
    start(). \
    awaitTermination()