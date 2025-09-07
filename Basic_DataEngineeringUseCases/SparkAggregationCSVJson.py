
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, col

# Create Spark session
spark = SparkSession.builder \
    .appName("SparkAggregationExample") \
    .master("local[*]") \
    .getOrCreate()

# ==============================
# 1. Load CSV (Salary Data)
# ==============================
salary_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./DataSets/sample_employees2.csv")

# ==============================
# 2. Load JSON (Employee Data)
# ==============================
employee_df = spark.read.json("./DataSets/sample_employees2.json")

# ==============================
# 3. Join DataFrames on EmpCode
# ==============================
joined_df = employee_df.join(salary_df, on="EmpCode", how="inner")

print("Joined Data:")
joined_df.show(truncate=False)

# ==============================
# 4. Aggregations
# ==============================

# Average salary per Designation
avg_salary_df = joined_df.groupBy("Designation").agg(avg("Salary").alias("AvgSalary"))
print("Average Salary per Designation:")
avg_salary_df.show(truncate=False)

# Maximum salary
max_salary_df = joined_df.agg(max("Salary").alias("MaxSalary"))
print("Maximum Salary:")
max_salary_df.show()

# Minimum salary
min_salary_df = joined_df.agg(min("Salary").alias("MinSalary"))
print("Minimum Salary:")
min_salary_df.show()

# Count employees per Designation
count_designation_df = joined_df.groupBy("Designation").agg(count("EmpCode").alias("EmployeeCount"))
print("Employee Count per Designation:")
count_designation_df.show(truncate=False)

# ==============================
# 5. Stop Spark
# ==============================
spark.stop()
