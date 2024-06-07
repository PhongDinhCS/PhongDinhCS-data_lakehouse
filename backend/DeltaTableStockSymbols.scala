import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DeltaStockSymbols {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Delta Lake Stock Symbols")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
      .getOrCreate()

    import spark.implicits._

    // Define the schema for the DataFrame
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("symbol", StringType, nullable = false)
    ))

    // Create a DataFrame with stock symbols and IDs
    val stockData = Seq(
      (1, "AAPL"),
      (2, "MSFT"),
      (3, "GOOGL"),
      (4, "AMZN"),
      (5, "FB"),
      (6, "TSLA"),
      (7, "BRK.A"),
      (8, "JNJ"),
      (9, "WMT"),
      (10, "JPM"),
      (11, "V"),
      (12, "PG"),
      (13, "NVDA"),
      (14, "DIS"),
      (15, "MA"),
      (16, "HD"),
      (17, "VZ"),
      (18, "NFLX"),
      (19, "PFE"),
      (20, "ADBE")
    ).toDF("id", "symbol")

    // Write DataFrame as a Delta Table
    stockData.write.format("delta").save("hdfs://namenode:8020/lakehouse/delta-stock-symbols")

    spark.stop()
  }
}

DeltaStockSymbols.main(Array()) // Call the main method
