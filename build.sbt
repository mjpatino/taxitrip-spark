scalaVersion := "2.12.15"

name := "TaxiTripApp"
version := "0.1"

scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
libraryDependencies ++= Seq(
  "org.scalameta" %% "munit" % "0.7.26" % Test,
  "org.apache.spark" %% "spark-core" % "3.3.3",
  "org.apache.spark" %% "spark-sql" % "3.3.3",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.2"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}