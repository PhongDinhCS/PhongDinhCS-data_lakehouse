name := "DeltaStockPriceReadLoop"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "io.delta" %% "delta-core" % "2.1.1"
)
