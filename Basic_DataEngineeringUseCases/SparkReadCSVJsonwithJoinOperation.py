
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkPractice_Join_CSV_JSON") \
    .master("local[*]") \
    .getOrCreate()

# Read CSV (with salaries)
employees_df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv("./DataSets/sample_employees2.csv")

# Read JSON (employee details)
employees_df_json = spark.read.json("./DataSets/sample_employees2.json")

# Perform inner join on EmpCode
joined_df = employees_df_csv.join(employees_df_json, on="EmpCode", how="inner")

print("Joined Data (CSV + JSON):")
joined_df.show(truncate=False)
