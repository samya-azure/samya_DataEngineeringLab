
import os
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

spark = SparkSession.builder \
    .appName("MongoSparkExample_NewAPI") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.read.database", "MongoSparkDB") \
    .config("spark.mongodb.read.collection", "tbl_Employee") \
    .getOrCreate()

# Load full collection
df = spark.read.format("mongodb").load()

# Force Spark to repartition into N partitions for parallelism
#df = df.repartition(8)   # You can tune this number

print("Total Records:", df.count())
print("Number of Spark Partitions:", df.rdd.getNumPartitions())

df.select("EmpCode", "EmployeeName").show(20, truncate=False)
