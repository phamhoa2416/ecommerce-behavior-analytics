import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.{MergeStrategy, PathList}

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

      "org.apache.httpcomponents" % "httpclient" % "4.5.13",
      "mysql" % "mysql-connector-java" % "8.0.33",
      "com.typesafe" % "config" % "1.4.3",
      "io.delta" %% "delta-spark" % "3.2.0",
      "com.clickhouse" % "clickhouse-jdbc" % "0.6.4",
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.2.1",
      "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1"
    ),
    assembly / mainClass := Some("com.example.batch.BATCH"),
    assembly / assemblyJarName := "ecommerce-batch-assembly.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.exists(name => {
            val lower = name.toLowerCase
            lower.endsWith(".sf") || lower.endsWith(".dsa") || lower.endsWith(".rsa")
          }) => MergeStrategy.discard
      case PathList("META-INF", "DEPENDENCIES") => MergeStrategy.discard
      case PathList("META-INF", "NOTICE.txt") => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case PathList("META-INF", "versions", _ @ _*) => MergeStrategy.first
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.filterDistinctLines
      case PathList("META-INF", "spring.schemas") => MergeStrategy.first
      case PathList("META-INF", "spring.handlers") => MergeStrategy.first
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case PathList("META-INF", "native-image", _ @ _*) => MergeStrategy.first
      case "reference.conf" | "application.conf" => MergeStrategy.concat
      case "arrow-git.properties" => MergeStrategy.first
      case PathList("mozilla", "public-suffix-list.txt") => MergeStrategy.first
      case PathList("google", "protobuf", _ @ _*) => MergeStrategy.first
      case PathList("META-INF", "FastDoubleParser-NOTICE") => MergeStrategy.first
      case PathList("META-INF", "org", "apache", "logging", "log4j", "core", "config", "plugins", "Log4j2Plugins.dat") =>
        MergeStrategy.concat
      case path if path.endsWith("LogFactory.class") => MergeStrategy.first
      case path if path.endsWith("Log.class") => MergeStrategy.first
      case path if path.endsWith("SimpleLog.class") => MergeStrategy.first
      case path if path.endsWith("LogConfigurationException.class") => MergeStrategy.first
      case path if path.endsWith("NoOpLog.class") => MergeStrategy.first
      case path if path.endsWith(".proto") => MergeStrategy.first
      case path if path.endsWith(".properties") => MergeStrategy.first
      case path if path.endsWith(".json") => MergeStrategy.first
      case _ => MergeStrategy.first
    }
  )