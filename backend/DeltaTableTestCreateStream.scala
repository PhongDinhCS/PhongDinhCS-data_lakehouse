
// scala
import org.apache.spark.sql.SparkSession
import io.delta.tables._

object DeltaOnHDFS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Delta Lake on HDFS")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
      .getOrCreate()

    // Create a DataFrame
    val data = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")

    // Write DataFrame as a Delta Table
    data.write.format("delta").save("hdfs://namenode:8020/lakehouse/delta-table-test")

    spark.stop()
  }
}

DeltaOnHDFS.main(Array()) // Call the main method