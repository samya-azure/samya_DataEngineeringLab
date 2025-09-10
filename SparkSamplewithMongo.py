from pyspark.sql import SparkSession

# MongoDB connection details
mongo_uri = "mongodb://localhost:27017"
database = "MongoSparkDB"
collection = "tbl_Employee"

# Create a function to start a Spark session and load specific data range
def fetch_data(skip_count, limit_count):
    spark = SparkSession.builder \
        .appName(f"MongoFetch_{skip_count}") \
        .config("spark.mongodb.read.connection.uri", mongo_uri) \
        .config("spark.mongodb.read.database", database) \
        .config("spark.mongodb.read.collection", collection) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .getOrCreate()

    # Load full collection
    df = spark.read.format("mongodb").load()

    # Add a row number for pagination
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, monotonically_increasing_id

    windowSpec = Window.orderBy(monotonically_increasing_id())
    df_with_index = df.withColumn("row_num", row_number().over(windowSpec))

    # Filter the range
    filtered_df = df_with_index.filter(
        (df_with_index.row_num > skip_count) & 
        (df_with_index.row_num <= skip_count + limit_count)
    )

    filtered_df.show(5, truncate=False)  # Just show first 5 rows
    print(f"Total rows in this batch: {filtered_df.count()}")

    spark.stop()

# Fetch data in 4 Spark sessions
batch_size = 250000
for i in range(4):
    skip_val = i * batch_size
    fetch_data(skip_val, batch_size)
