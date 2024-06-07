import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables._
import scala.util.Random

object UpdateDeltaStockPrices {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Update Delta Stock Prices")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
      .getOrCreate()

    import spark.implicits._

    def updatePrices(): Unit = {
      try {
        // Read the existing data from the delta-stock-prices table
        val deltaTablePath = "hdfs://namenode:8020/lakehouse/delta-stock-prices"
        val deltaTable = DeltaTable.forPath(spark, deltaTablePath)
        val stockData = deltaTable.toDF

        // Generate random prices for each stock ID, with some prices as strings
        val updatedStockData = stockData.withColumn("price", 
          when(rand() < 0.1, lit(Random.alphanumeric.take(5).mkString)).otherwise(round(rand() * 1000 + 1, 2))
        )

        // Update the delta-stock-prices table with the new prices
        deltaTable.as("old")
          .merge(
            updatedStockData.as("new"),
            "old.id = new.id"
          )
          .whenMatched()
          .updateAll()
          .execute()

        println("Update succeeded")

      } catch {
        case e: Exception =>
          println(s"Update failed: ${e.getMessage}")
      }
    }

    // Loop to update the Delta table every 5 seconds
    while (true) {
      updatePrices()
      Thread.sleep(5000) // Sleep for 5 seconds
    }

    // This line will never be reached due to the infinite loop
    // spark.stop()
  }
}

UpdateDeltaStockPrices.main(Array()) // Call the main method
