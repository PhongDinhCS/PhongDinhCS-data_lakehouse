from pyspark.sql import SparkSession
from flask import Flask, jsonify

# Initialize Spark session with Delta configurations
spark = SparkSession.builder \
    .appName("DeltaLakeAPI") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define path to Delta table
delta_table_path = "hdfs://namenode:8020/testlakehouse/TestStockSymbols"

# Initialize Flask app
app = Flask(__name__)

# Define API route
@app.route('/api/stocks', methods=['GET'])
def get_stocks():
    # Load Delta table
    df = spark.read.format("delta").load(delta_table_path)
    
    # Convert DataFrame to JSON
    data = df.toPandas().to_dict(orient='records')
    
    return jsonify(data)

# Run Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
