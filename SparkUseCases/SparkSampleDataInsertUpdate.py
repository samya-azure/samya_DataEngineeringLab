
import os

# Ensure Spark uses the correct Python interpreter
os.environ["PYSPARK_PYTHON"] = r"C:\Users\samya.basu\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\samya.basu\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Step 1: Create SparkSession in local mode
spark = SparkSession.builder \
    .appName("InsertUpdateDisplayExample") \
    .master("local[*]") \
    .getOrCreate()

# Step 2: Initial Data
data = [("Samya Basu", "Data Engineer", 5),
        ("John Dicosta", "Developer", 3)]
columns = ["Name", "Role", "Experience"]

df = spark.createDataFrame(data, columns)
print("Initial Data:")
df.show()

# Step 3: INSERT (add new row)
new_data = [("Rony Rene", "Manager", 10)]
new_df = spark.createDataFrame(new_data, columns)
df = df.union(new_df)

print("After Insert:")
df.show()

# Step 4: UPDATE (change Role where Name == 'John Dicosta')
df = df.withColumn("Role", when(df.Name == "John Dicosta", "Senior Developer").otherwise(df.Role))

print("After Update:")
df.show()

# Step 5: SELECT / DISPLAY
print("People with > 4 years of experience:")
df.filter(df.Experience > 4).show()

# Step 6: Stop Spark
spark.stop()
