object DeltaSparkTest {
  def main(args: Array[String]): Unit = {
    // Import required Spark classes
    import org.apache.spark.sql.SparkSession

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Delta Spark Test")
      .getOrCreate()

    try {
      // Create a simple DataFrame
      val data = Seq(("Alice", 34), ("Bob", 45), ("Charlie", 27))
      val df = spark.createDataFrame(data).toDF("Name", "Age")

      // Show the DataFrame
      df.show()

      // Stop the SparkSession
      spark.stop()
    } catch {
      case e: Exception => 
        // Handle any exceptions
        println(s"An error occurred: ${e.getMessage}")
    }
  }
}
