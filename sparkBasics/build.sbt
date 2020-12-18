name := "sparkBasics"

version := "0.1"

scalaVersion := "2.12.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// https://mvnrepository.com/artifact/com.databricks/spark-csv
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
