net.virtualvoid.sbt.graph.Plugin.graphSettings

name := "taxispark"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.2.0" % "provided"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "1.9.1" % "provided"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-simpledb" % "1.9.19"

lazy val commonSettings = Seq(
  version := "1.0",
  organization := "com.kevin",
  scalaVersion := "2.11.4"
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*)

mainClass in assembly := Some("Taxi8")