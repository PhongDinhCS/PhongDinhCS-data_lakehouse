import org.apache.spark.sql.SparkSession
import io.delta.tables._

object DeltaOnHDFS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Delta Lake on HDFS")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
      .getOrCreate()

    // Tạo DataFrame
    val data = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")

    // Ghi DataFrame dưới dạng Delta Table
    data.write.format("delta").save("hdfs://namenode:8020/delta-table")

    spark.stop()
  }
}
