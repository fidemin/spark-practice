ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "spark-practice",
    idePackagePrefix := Some("com.yunhongmin")
  )

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
  "org.apache.spark" %% "spark-core" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.4.8"
)
