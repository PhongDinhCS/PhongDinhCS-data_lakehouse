import org.apache.spark.sql.SparkSession
import io.delta.tables._
import scala.concurrent.duration._

object LoopReadDeltaStockPrices {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Loop Read Delta Stock Prices")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
      .getOrCreate()

    // Function to read and display the Delta table
    def readDeltaTable(): Unit = {
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
      }
    }

    // Loop to read the Delta table every 5 seconds
    while (true) {
      readDeltaTable()
      Thread.sleep(5000) // Sleep for 5 seconds
    }

    // This line will never be reached due to the infinite loop
    // spark.stop()
  }
}

LoopReadDeltaStockPrices.main(Array()) // Call the main method
