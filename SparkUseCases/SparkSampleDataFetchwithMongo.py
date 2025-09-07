
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MongoSparkExample_NewAPI") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/MongoSparkDB.tbl_Employee") \
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/MongoSparkDB.tbl_Employee") \
    .getOrCreate()

df = spark.read.format("mongodb").load()
df.show(10, truncate=False)
df.printSchema()
