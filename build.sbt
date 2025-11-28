ThisBuild / organization := "com.lakehouse"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.4.1"

lazy val root = (project in file("."))
  .settings(
    name := "lakehouse-ingestion",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"                 % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"                  % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive"                 % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
      // Needed for DataFrame-based Kafka source in KafkaReader
      "org.apache.spark" %% "spark-sql-kafka-0-10"       % sparkVersion % "provided",
      "org.apache.hadoop" %  "hadoop-aws"                % "3.3.2",
      "io.delta"         %% "delta-core"                 % "2.4.0",
      "io.delta"          % "delta-storage"              % "2.4.0",
      "org.apache.iceberg" % "iceberg-spark"             % "1.5.1",
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.4" % "1.5.1",
      "com.amazonaws"      % "aws-java-sdk-bundle"       % "1.12.262",
      "org.slf4j"          % "slf4j-api"                 % "1.7.36",
      "org.slf4j"          % "slf4j-simple"              % "1.7.36",
      "org.postgresql"     % "postgresql"                % "42.7.3",
      "com.typesafe"       % "config"                    % "1.4.3"
    )
  )

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "." + artifact.extension
}