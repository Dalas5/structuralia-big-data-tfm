// Package Information

ThisBuild / organization := "com.dalas"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "amazon-cfr",
    Compile / mainClass := Some("com.dalas.amazon_cfr"),
    packageBin / mainClass := Some("com.dalas.amazon_cfr"),
    assembly / mainClass := Some("com.dalas.amazon_cfr")
  )

// Spark Information
val sparkVersion = "3.3.0"

// allows us to include spark packages
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases/"
resolvers += "MavenRepository" at "https://mvnrepository.com/"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // testing
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.scalacheck" %% "scalacheck" % "1.17.0" % "test",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.19.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.19.0",

  // https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3-bundle
  "org.apache.hudi" %% "hudi-spark3-bundle" % "0.12.2",

  // https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.398",

  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",

  // Hudi
  "org.apache.hudi" %% "hudi-spark3-bundle" % "0.12.2",

  // ML
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "4.2.1",

)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}