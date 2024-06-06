import org.apache.spark.sql.SparkSession

// Specify the namenode address
val namenodeAddress = "hdfs://namenode:8020"

// Initialize SparkSession
val spark = SparkSession.builder
  .appName("CreateDeltaTable")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .config("spark.hadoop.fs.defaultFS", namenodeAddress) // Specify namenode address
  .getOrCreate()

// Sample data
val data = Seq(
  ("AAPL", "2024-06-03", 135.24),
  ("GOOGL", "2024-06-03", 2550.67),
  ("MSFT", "2024-06-03", 249.68),
  ("AMZN", "2024-06-03", 3286.33)
)

// Define DataFrame schema
val columns = Seq("Symbol", "Date", "Price")
val df = spark.createDataFrame(data).toDF(columns: _*)

// Specify HDFS path for Delta Lake table
val deltaPath = "/raw"

// Write DataFrame to Delta Lake format
df.write.format("delta").save(deltaPath)

// Check Delta Lake table
val deltaDF = spark.read.format("delta").load(deltaPath)
deltaDF.show()

// Check Delta Lake log
val deltaLogDF = spark.read.format("delta").option("versionAsOf", 0).load(deltaPath + "/_delta_log")
deltaLogDF.show()
