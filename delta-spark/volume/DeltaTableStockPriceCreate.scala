import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._

object DeltaStockPrices {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Delta Lake Stock Prices")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
      .getOrCreate()

    import spark.implicits._

    // Define the schema for the DataFrame
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("price", DoubleType, nullable = false)
    ))

    // Create a DataFrame with stock IDs and prices
    val stockData = Seq(
      (1, 150.25),
      (2, 299.35),
      (3, 2750.45),
      (4, 3500.75),
      (5, 325.15),
      (6, 695.60),
      (7, 414500.00),
      (8, 163.70),
      (9, 144.50),
      (10, 158.90),
      (11, 233.35),
      (12, 141.55),
      (13, 230.75),
      (14, 185.35),
      (15, 363.25),
      (16, 335.45),
      (17, 58.75),
      (18, 515.35),
      (19, 44.85),
      (20, 550.95)
    ).toDF("id", "price")

    // Load the delta-stock-symbols table
    val symbols = spark.read.format("delta").load("hdfs://namenode:8020/lakehouse/delta-stock-symbols")

    // Join the stockData with symbols to ensure valid IDs
    val validStockData = stockData.join(symbols, Seq("id"), "inner")

    // Write DataFrame as a Delta Table
    validStockData.write.format("delta").save("hdfs://namenode:8020/lakehouse/delta-stock-prices")

    spark.stop()
  }
}

DeltaStockPrices.main(Array()) // Call the main method
