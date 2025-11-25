ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "ecommerce-behavior-analysis",
    Compile / resourceDirectory := baseDirectory.value / "src" / "main" / "resources",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-streaming" % "3.5.1",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.793",
      "com.starrocks" % "starrocks-spark-connector-3.5_2.12" % "1.1.3",
      "org.apache.httpcomponents" % "httpclient" % "4.5.13",
      "mysql" % "mysql-connector-java" % "8.0.33",
      "com.oracle.database.jdbc" % "ojdbc8" % "19.3.0.0",
      "com.typesafe" % "config" % "1.4.3",
      "io.delta" %% "delta-spark" % "3.2.0",
      "com.clickhouse" % "clickhouse-jdbc" % "0.6.4",
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.2.1",
      "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1"
    )
  )