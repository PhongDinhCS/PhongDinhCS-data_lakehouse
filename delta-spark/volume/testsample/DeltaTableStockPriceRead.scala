import org.apache.spark.sql.SparkSession
import io.delta.tables._

object ReadDeltaStockPrices {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read Delta Stock Prices")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
      .getOrCreate()

    try {
      // Read the Delta table
      val deltaTablePath = "hdfs://namenode:8020/lakehouse/delta-stock-prices"
      val deltaTable = DeltaTable.forPath(spark, deltaTablePath)
      val stockData = deltaTable.toDF

      // Show the contents of the table
      stockData.show()

      // Print schema of the table
      stockData.printSchema()

    } catch {
      case e: Exception =>
        println(s"Failed to read Delta table: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }
}

ReadDeltaStockPrices.main(Array()) // Call the main method
