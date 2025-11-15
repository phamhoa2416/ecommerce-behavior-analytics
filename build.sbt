ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "ECommerceStream",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.7",
      "org.apache.spark" %% "spark-sql" % "3.5.7",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.7",
      "com.clickhouse"   %  "clickhouse-jdbc" % "0.6.0"
    )
  )