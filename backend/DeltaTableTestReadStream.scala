import org.apache.spark.sql.SparkSession

object ReadDeltaFromHDFS {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Read Delta Table from HDFS")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
      .getOrCreate()

    // Load Delta table
    val deltaDF = spark.read.format("delta").load("hdfs://namenode:8020/lakehouse/delta-table-test")

    // Show DataFrame
    deltaDF.show()

    // Stop SparkSession
    spark.stop()
  }
}

ReadDeltaFromHDFS.main(Array())
