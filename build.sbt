name := "flink-essentials"

version := "0.1"

scalaVersion := "2.12.15"

val flinkVersion = "1.13.2"
val postgresVersion = "42.2.2"
val logbackVersion = "1.2.10"

val flinkDependencies = Seq(
  "org.apache.flink" % "flink-clients_2.12" % flinkVersion,
  "org.apache.flink" % "flink-scala_2.12" % flinkVersion,
  "org.apache.flink" % "flink-streaming-scala_2.12" % flinkVersion
)

val flinkConnectors = Seq(
  "org.apache.flink" % "flink-connector-kafka_2.12" % flinkVersion,
  "org.apache.flink" % "flink-connector-cassandra_2.12" % flinkVersion,
  "org.apache.flink" % "flink-connector-jdbc_2.12" % flinkVersion,
  "org.postgresql" % "postgresql" % postgresVersion
)

val logging = Seq(
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion
)

libraryDependencies ++= flinkDependencies ++ flinkConnectors ++ logging
