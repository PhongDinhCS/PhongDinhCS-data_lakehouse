import org.apache.spark.sql.SparkSession
import io.delta.tables._

object DeltaTableTestRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Delta Lake Test Read")
      .getOrCreate()

    // Load Delta table as DataFrame
    val deltaTableDF = DeltaTable.forPath(spark, "hdfs://namenode:8020/lakehouse/delta-table-test")

    // Display the history of the Delta table
    deltaTableDF.history().show()

    spark.stop()
  }
}
import org.apache.spark.sql.SparkSession
import io.delta.tables._

object DeltaTableTestRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Delta Lake Test Read")
      .getOrCreate()

    // Load Delta table as DataFrame
    val deltaTableDF = DeltaTable.forPath(spark, "hdfs://namenode:8020/lakehouse/delta-table-test")

    // Display the history of the Delta table
    deltaTableDF.history().show()

    spark.stop()
  }
}
