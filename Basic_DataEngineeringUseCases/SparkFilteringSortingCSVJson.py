
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Spark_Filter_Sort_Example") \
    .master("local[*]") \
    .getOrCreate()

# =======================
# Load CSV (Salary Data)
# =======================
salary_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("./DataSets/sample_employees2.csv")

# =======================
# Load JSON (Employee Data)
# =======================
employee_df = spark.read.option("multiline", "false").json("./DataSets/sample_employees2.json")

print("Employee Details:")
employee_df.show(truncate=False)

print("Salary Details:")
salary_df.show(truncate=False)

# =======================
# Join Employee + Salary on EmpCode
# =======================
joined_df = employee_df.join(salary_df, on="EmpCode", how="inner")

print("Joined Data:")
joined_df.show(truncate=False)

# =======================
# Filtering: Salary > 100000
# =======================
high_salary_df = joined_df.filter(joined_df["Salary"] > 100000)

print("Employees with Salary > 100000:")
high_salary_df.show(truncate=False)

# =======================
# Sorting: Order by Salary Descending
# =======================
sorted_df = joined_df.orderBy(joined_df["Salary"].desc())

print("Employees Sorted by Salary (High to Low):")
sorted_df.show(truncate=False)

# =======================
# Filtering + Sorting together
# =======================
filtered_sorted_df = joined_df.filter(joined_df["Salary"] > 80000) \
                              .orderBy(joined_df["Salary"].desc())

print("Employees with Salary > 80000, Sorted High to Low:")
filtered_sorted_df.show(truncate=False)

# Stop Spark
spark.stop()
