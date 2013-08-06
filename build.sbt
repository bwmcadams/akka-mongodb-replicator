name := "akka-mongodb-replicator"

organization := "com.typesafe.akka"

version := "1.0.0"

scalaVersion := "2.9.1"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases"

libraryDependencies ++= Seq(
    "com.typesafe.akka" % "akka-actor" % "2.0.5" % "provided",
    "org.mongodb" %% "casbah" % "2.6.1",
    // test dependencies
    "com.typesafe.akka" % "akka-testkit" % "2.0.5" % "test",
    "org.specs2" %% "specs2" % "1.12.3" % "test",
    "junit" % "junit" % "4.7" % "test"
)
