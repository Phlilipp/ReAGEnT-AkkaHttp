name := "ReAGEnT-AkkaHttp"

version := "0.1"

scalaVersion := "2.12.13"

val AkkaVersion = "2.6.8"
val AkkaHttpVersion = "10.2.4"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "org.reactivemongo" %% "reactivemongo" % "1.0.3",
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.2.3"
)
