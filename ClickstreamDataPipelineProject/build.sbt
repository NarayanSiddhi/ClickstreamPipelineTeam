ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "ClickstreamDataPipelineProject",
    libraryDependencies += "com.typesafe" % "config" % "1.4.2"
  )
