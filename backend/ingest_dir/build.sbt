name := "KafkaToHDFS"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.apache.spark" %% "spark-streaming" % "3.3.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.1",
  "org.apache.kafka" %% "kafka" % "2.8.0"
)
