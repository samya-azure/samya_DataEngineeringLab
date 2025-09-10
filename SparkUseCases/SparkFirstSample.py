import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\samya.basu\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\samya.basu\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession


# Step 1: Create SparkSession in local mode
'''spark = SparkSession.builder \
    .appName("LocalSparkExample") \
    .master("local[*]").getOrCreate()'''


spark = SparkSession.builder.master("local[*]").appName("FixPythonError").getOrCreate()

# Step 2: Create a small dataset
data = [("Samya", "Data Engineer", 5),
        ("John", "Developer", 3),
        ("Rony", "Manager", 10)]

columns = ["Name", "Role", "Experience"]

df = spark.createDataFrame(data, columns)

# Step 3: Show the DataFrame
print("Full DataFrame:")
df.show()

# Step 4: Filter (people with > 4 years of experience)
print("People with more than 5 years of experience:")
df.filter(df.Experience > 5).show()

# Step 5: Run SQL queries
df.createOrReplaceTempView("employees")
sql_df = spark.sql("SELECT Name, Role FROM employees WHERE Experience > 5")

print("SQL Query Result:")
sql_df.show()

# Step 6: Stop Spark
spark.stop()
