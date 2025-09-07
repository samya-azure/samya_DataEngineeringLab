
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkPractice_CSV_JSON") \
    .master("local[*]") \
    .getOrCreate()

# Read CSV
employees_df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv("./DataSets/sample_employees.csv")

# Read JSON
employees_df_json = spark.read.json("./DataSets/sample_employees.json")
print("Employees Data from CSV:")
employees_df_csv.show(truncate=False)

print("Employees Data from Json:")
employees_df_json.show(truncate=False)
