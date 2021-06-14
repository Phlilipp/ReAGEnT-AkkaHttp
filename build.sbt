
name := "ReAGEnT-AkkaHttp"

version := "0.1"

scalaVersion := "2.12.13"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

val AkkaVersion = "2.6.8"
val AkkaHttpVersion = "10.2.4"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "org.reactivemongo" %% "reactivemongo" % "1.0.3",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
)
