
from pyspark.sql import SparkSession
import os

# Create Spark Session
spark = SparkSession.builder \
    .appName("SparkPractice_WriteData") \
    .master("local[*]") \
    .getOrCreate()

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + os.path.join("C:\\hadoop", "bin")
# ==============================
# 1. Read input datasets
# ==============================
employees_csv = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("./DataSets/sample_employees2.csv")

employees_json = spark.read.json("./DataSets/sample_employees2.json")

print("CSV Data (Salaries):")
employees_csv.show(truncate=False)

print("JSON Data (Employees):")
employees_json.show(truncate=False)

# ==============================
# 2. Write Data in Different Formats
# ==============================

# ---- Write CSV (Overwrite) ----
#employees_csv.write.mode("overwrite").option("header", "true").csv("./Output_Files/Employees_CSV")
employees_csv.toPandas().to_csv("./Output_Files/Employees_CSV/employees.csv", index=False)

# ---- Write JSON (Overwrite) ----
#employees_json.write.mode("overwrite").json("./Output_Files/Employees_JSON")
employees_json.toPandas().to_json("./Output_Files/Employees_Json/employees.json", orient="records", lines=True)

# ---- Write Parquet (Overwrite) ----
#employees_json.write.mode("overwrite").parquet("./Output_Files/Employees_Parquet")
employees_csv.toPandas().to_parquet("./Output_Files/Employees_Parquet/employees.parquet", index=False)

print("Data written successfully in CSV, JSON, and Parquet (overwrite mode)")

# ==============================
# 3. Try Append Mode
# ==============================
# For demo, let’s filter few employees and append
filtered_df = employees_json.filter(employees_json["YearOfExperience"] > 10)

# Append to same Parquet folder
filtered_df.write.mode("append").parquet("./Output_Files/Employees_Parquet")

print("Filtered data appended successfully to Parquet")

# ==============================
# 4. Read Back and Verify
# ==============================
print("Reading back from Parquet after append:")
parquet_df = spark.read.parquet("./Output_Files/Employees_Parquet")
parquet_df.show(truncate=False)

# Stop Spark
spark.stop()
