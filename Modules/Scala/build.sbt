name := "job-metrics-listener"

version := "0.2.0"

scalaVersion := "2.12.17" // Match your Spark version (e.g., Databricks = Scala 2.12)

val sparkVersion = "3.4.1" // Or the version you're using

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)

val deltaVersion = "2.4.0"

libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"