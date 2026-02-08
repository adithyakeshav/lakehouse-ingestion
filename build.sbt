ThisBuild / organization := "com.lakehouse"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.4.1"

lazy val root = (project in file("."))
  .settings(
    name := "lakehouse-ingestion",

    // Include schemas directory in JAR resources
    Compile / unmanagedResourceDirectories += baseDirectory.value / "schemas",
    Compile / unmanagedResourceDirectories += baseDirectory.value / "configs",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"                 % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"                  % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive"                 % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
      // Needed for DataFrame-based Kafka source in KafkaReader
      "org.apache.spark" %% "spark-sql-kafka-0-10"       % sparkVersion % "provided",
      // Runtime deps: downloaded via spark.jars.packages (not bundled in fat JAR)
      "org.apache.hadoop" %  "hadoop-aws"                % "3.3.2"  % "provided",
      "com.amazonaws"      % "aws-java-sdk-bundle"       % "1.12.262" % "provided",
      "io.delta"         %% "delta-core"                 % "2.4.0"  % "provided",
      "io.delta"          % "delta-storage"              % "2.4.0"  % "provided",
      "org.apache.iceberg" % "iceberg-spark"             % "1.5.1"  % "provided",
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.4" % "1.5.1" % "provided",
      "org.slf4j"          % "slf4j-api"                 % "1.7.36",
      "org.slf4j"          % "slf4j-simple"              % "1.7.36",
      "org.postgresql"     % "postgresql"                % "42.7.3",
      "com.typesafe"       % "config"                    % "1.4.3"
    )
  )

assembly / assemblyJarName := "lakehouse-ingestion.jar"

// Minimal assembly merge strategy (Spark provided at runtime via image)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", "versions", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", "org", "apache", "logging", "log4j", "core", "config", "plugins", "Log4j2Plugins.dat") => MergeStrategy.first
  case "reference.conf" => MergeStrategy.concat
  case "module-info.class" => MergeStrategy.discard
  case PathList("mozilla", "public-suffix-list.txt") => MergeStrategy.first
  case x =>
    val old = (assembly / assemblyMergeStrategy).value
    old(x)
}
