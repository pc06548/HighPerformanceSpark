name := "HighPerformanceSpark"

version := "0.1"

scalaVersion := "2.11.06"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-hive" % "2.0.0" )