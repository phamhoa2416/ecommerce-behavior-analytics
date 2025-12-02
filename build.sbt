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
      "io.delta" %% "delta-spark" % "3.2.0",

      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.793",

      "com.typesafe" % "config" % "1.4.3",

      "com.clickhouse" % "clickhouse-jdbc" % "0.6.4",
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.2.1",
      "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1"
    ),

    assembly / mainClass := Some("com.example.batch.BATCH"),
    assembly / mainClass := Some("com.example.streaming.STREAMING"),
    assembly / assemblyJarName := "ecommerce-batch-assembly.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("META-INF", "versions", _ @ _*) => MergeStrategy.first
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.filterDistinctLines
      case "reference.conf" | "application.conf" => MergeStrategy.concat
      case PathList("META-INF", "org", "apache", "logging", "log4j", "core", "config", "plugins", "Log4j2Plugins.dat") =>
        MergeStrategy.concat
      case x => MergeStrategy.first
    }
  )