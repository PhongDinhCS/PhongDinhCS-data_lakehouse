import psycopg2
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import time
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AppendDataToTestStockTransaction") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .getOrCreate()


# Define the Delta path for TestStockTransaction
delta_path_TestStockTransaction = "hdfs://namenode:8020/testlakehouse/TestStockTransaction"
delta_path_TestStockCurrentPrice = "hdfs://namenode:8020/testlakehouse/TestStockCurrentPrice"

while True:
    try:
        # Connect to PostgreSQL and fetch a random row
        conn = psycopg2.connect(
            dbname="bqnbcj8kxuogsigyhnzy",
            user="ufjklpchveyybgraqhxu",
            password="AyR5dzFuySPaAcWd5po1AJMK063nkG",
            host="bqnbcj8kxuogsigyhnzy-postgresql.services.clever-cloud.com",
            port="50013"
        )
        cur = conn.cursor()

        # Fetch a random row from PostgreSQL
        cur.execute("""
            SELECT * FROM sample_stock_data ORDER BY RANDOM() LIMIT 1
        """)

        # Fetch the row
        random_row = cur.fetchone()

    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

    # Define the columns
    columns = [
        'AAPL_price', 'AAPL_volume', 'AMZN_price', 'AMZN_volume', 
        'GOOG_price', 'GOOG_volume', 'MSFT_price', 'MSFT_volume',
        'NVDA_price', 'NVDA_volume', 'TSLA_price', 'TSLA_volume'
    ]

    # Convert the row to a DataFrame
    row_df = pd.DataFrame([random_row], columns=columns)

    # Convert to Spark DataFrame
    row_spark_df = spark.createDataFrame(row_df)

    # Convert the data types
    row_spark_df_corrected = row_spark_df \
        .withColumn("AAPL_price", col("AAPL_price").cast("double")) \
        .withColumn("AAPL_volume", col("AAPL_volume").cast("long")) \
        .withColumn("AMZN_price", col("AMZN_price").cast("double")) \
        .withColumn("AMZN_volume", col("AMZN_volume").cast("long")) \
        .withColumn("GOOG_price", col("GOOG_price").cast("double")) \
        .withColumn("GOOG_volume", col("GOOG_volume").cast("long")) \
        .withColumn("MSFT_price", col("MSFT_price").cast("double")) \
        .withColumn("MSFT_volume", col("MSFT_volume").cast("long")) \
        .withColumn("NVDA_price", col("NVDA_price").cast("double")) \
        .withColumn("NVDA_volume", col("NVDA_volume").cast("long")) \
        .withColumn("TSLA_price", col("TSLA_price").cast("double")) \
        .withColumn("TSLA_volume", col("TSLA_volume").cast("long"))

    # Add the timestamp column
    row_spark_df_with_timestamp = row_spark_df_corrected.withColumn("timestamp", current_timestamp())

    # Append data to the TestStockTransaction Delta table
    row_spark_df_with_timestamp.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(delta_path_TestStockTransaction)

    print(f"Data appended to TestStockTransaction successfully. Timestamp: {datetime.now()}")

    ###########################################################################

    # Transform the DataFrame from wide to long format
    melted_df = row_spark_df.selectExpr(
        "stack(6, 'AAPL', AAPL_price, 'AMZN', AMZN_price, 'GOOG', GOOG_price, 'MSFT', MSFT_price, 'NVDA', NVDA_price, 'TSLA', TSLA_price) as (symbol, price)"
    ).join(
        row_spark_df.selectExpr(
            "stack(6, 'AAPL', AAPL_volume, 'AMZN', AMZN_volume, 'GOOG', GOOG_volume, 'MSFT', MSFT_volume, 'NVDA', NVDA_volume, 'TSLA', TSLA_volume) as (symbol, volume)"
        ),
        on="symbol"
    ).select("symbol", "price", "volume")

    # Convert columns to the appropriate data types
    melted_df_corrected = melted_df \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("volume", col("volume").cast("long"))

    # Write the corrected DataFrame to the Delta table
    melted_df_corrected.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(delta_path_TestStockCurrentPrice)

    print(f"Delta table updated TestStockCurrentPrice successfully. Timestamp: {datetime.now()}")
    

    # Wait for 10 seconds before the next iteration
    time.sleep(10)
