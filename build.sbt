import sbtassembly.AssemblyPlugin.autoImport.assemblyMergeStrategy
import sbtassembly.MergeStrategy

ideaExcludeFolders += ".idea"
ideaExcludeFolders += ".idea_modules"

val scala_version = "2.10.6"
val kafka_version = "0.10.0.0"
val spark_version = "2.1.0"

val projectName = "spark-streaming-util"

val commonSettings = Seq(
  version := "1.0",
  organization := "https://github.com/lzhshen",
  scalaVersion := scala_version,
  fork := true,
  parallelExecution in Test := false,
  cancelable in Global := true
)

val customMergeStrategy: String => MergeStrategy = {
  case PathList("local.conf") => MergeStrategy.discard
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
  case PathList("org", "apache", "commons", xs @ _ *) => MergeStrategy.last
  case PathList("org", "apache", "hadoop", "yarn", xs @ _ *) => MergeStrategy.last
  case PathList("org", "apache", "spark", xs @ _ *) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _ *) => MergeStrategy.last
  case PathList("javax","inject", xs @ _ *) => MergeStrategy.last
  case "overview.html" => MergeStrategy.discard
  case s =>
    MergeStrategy.defaultMergeStrategy(s)
}

val commonScalacOptions = Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen"
  // "-Ywarn-unused-import"
)

val commonLibraryDependencies = Seq(
  //"org.scala-lang" % "scala-compiler" % Versions.scala,
  //"org.scala-lang" % "scala-library" % Versions.scala,
  //"org.scala-lang" % "scala-reflect" % Versions.scala,

  "org.apache.kafka" %% "kafka" % kafka_version,
  "org.apache.kafka" % "kafka-clients" % kafka_version,


  "org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-streaming" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % spark_version,
  "com.github.benfradet" %% "spark-kafka-0-10-writer" % "0.2.0",

  "org.slf4j" % "slf4j-log4j12" % "1.7.21" % "runtime",
  "org.slf4j" % "slf4j-api" % "1.7.21" % "provided",
  "org.slf4j" % "slf4j-nop" % "1.7.21" % "test",

  "com.typesafe" % "config" % "1.2.1",
  //"net.ceedubs" %% "ficus" % "1.1.2",
  "net.ceedubs" %% "ficus" % "1.0.1",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)


assemblyMergeStrategy in assembly := customMergeStrategy

lazy val main = Project(projectName, base = file("."))
  .settings(commonSettings: _*)
  //.settings(assemblySettings: _*)
  .settings(scalacOptions ++= commonScalacOptions)
  .settings(libraryDependencies ++= commonLibraryDependencies)
//.settings(excludeDependencies ++= commonExcludeDependencies)

(dependencyClasspath in Test) := (dependencyClasspath in Test).map(
  _.filterNot(_.data.name.contains("slf4j-log4j12"))
).value
