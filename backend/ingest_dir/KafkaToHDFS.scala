import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object KafkaToHDFS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Kafka to HDFS")
      .master("local[*]") // Change to your cluster master
      .getOrCreate()

    // Kafka parameters
    val kafkaBootstrapServers = "kafka:9092"
    val kafkaTopic = "html_topic"

    // Define the Kafka source
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Assuming the value contains Parquet data as bytes
    val parquetDF = kafkaDF.selectExpr("CAST(value AS BINARY) as parquet_data")

    // Process and write the data to HDFS
    val query = parquetDF.writeStream
      .foreachBatch { (batchDF, batchId) =>
        batchDF.persist()

        // Write each batch to HDFS as parquet
        batchDF.write
          .mode("append")
          .parquet(s"hdfs://namenode:9000/raw/parquet_data_$batchId.parquet")

        batchDF.unpersist()
      }
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    query.awaitTermination()
  }
}
