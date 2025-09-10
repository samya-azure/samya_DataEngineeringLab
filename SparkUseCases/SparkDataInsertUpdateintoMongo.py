

import os
from pyspark.sql import SparkSession, Row
from pymongo import MongoClient

# Fix Python executable issue
os.environ["PYSPARK_PYTHON"] = r"C:\Users\samya.basu\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\samya.basu\AppData\Local\Programs\Python\Python311\python.exe"

# Set Hadoop path (needed for Windows)
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

# Create SparkSession
spark = SparkSession.builder \
    .appName("MongoSparkExample_NewAPI") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.read.database", "MongoSparkDB") \
    .config("spark.mongodb.read.collection", "tbl_Employee") \
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.write.database", "MongoSparkDB") \
    .config("spark.mongodb.write.collection", "tbl_Employee") \
    .getOrCreate()

# ==============================
# 1. Load existing collection
# ==============================
df = spark.read.format("mongodb").load()
print("Total Records before insert:", df.count())

# ==============================
# 2. Insert new records (EmpCode as _id)
# ==============================
new_data = [
    Row(EmpCode="EMP999999", EmployeeName="Samya Basu", Designation="Technical Architect", YearOfExperience=12),
    Row(EmpCode="EMP999991", EmployeeName="Johny Deep", Designation="Tester", YearOfExperience=8)
]
new_df = spark.createDataFrame(new_data).withColumnRenamed("EmpCode", "_id")

# Insert with EmpCode as primary key (_id)
new_df.write.format("mongodb").mode("append").save()
print("Insert completed")

# ==============================
# 3. Update / Upsert records (also EmpCode → _id)
# ==============================
update_data = [
    Row(EmpCode="EMP999999", EmployeeName="Samya Basu", Designation="Azure Data Engineer", YearOfExperience=13),
    Row(EmpCode="EMP999991", EmployeeName="Sylvester Stallone", Designation="Senior Tester", YearOfExperience=9)
]
update_df = spark.createDataFrame(update_data).withColumnRenamed("EmpCode", "_id")

update_df.write \
    .format("mongodb") \
    .mode("append") \
    .option("replaceDocument", "true") \
    .save()
print("Upsert completed")

# ==============================
# 4. Verify results
# ==============================

final_df = spark.read \
    .format("mongodb") \
    .option("uri", "mongodb://127.0.0.1/") \
    .option("database", "MongoSparkDB") \
    .option("collection", "tbl_Employee") \
    .load()

# To get actual record count us pymongo
client = MongoClient("mongodb://127.0.0.1/")
db = client["MongoSparkDB"]
coll = db["tbl_Employee"]

print("Record Count from MongoDB:", coll.count_documents({}))

#print("Accurate Record Count:", final_df.count())
#print("Total Records after Insert/Update:", final_df.count())
final_df.filter(final_df["_id"].isin("EMP999999", "EMP999991")).show(truncate=False)

# Stop Spark
spark.stop()
