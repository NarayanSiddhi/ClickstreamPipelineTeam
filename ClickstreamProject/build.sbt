ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "clickstream_data", libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.7",
      "org.apache.spark" %% "spark-sql" % "2.4.7"),
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test,
    libraryDependencies += "com.typesafe" % "config" % "1.4.1"

  )
