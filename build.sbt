ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "clickstream_data", libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.7",
      "org.apache.spark" %% "spark-sql" % "2.4.7"),
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test,
    libraryDependencies += "com.typesafe" % "config" % "1.4.1",
    // Dependencies
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17"
      // Other dependencies here
    )
  )

// Main class declaration
mainClass in(Compile, run) := Some("main.scala.ClickstreamPipeLine")
