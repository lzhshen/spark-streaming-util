// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import sbt.Keys._
import sbt._

object ProjectBuild extends Build {

  object Versions {
    val kafka = "0.10.0.0"
    val spark = "2.1.0"
    val scala = "2.11.8"
  }

  val projectName = "spark-streaming-util"

  val commonSettings = Seq(
    version := "1.0",
    organization := "http://mkuthan.github.io/",
    scalaVersion := Versions.scala,
    fork := true,
    parallelExecution in Test := false,
    cancelable in Global := true
  )

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
    "org.scala-lang" % "scala-compiler" % Versions.scala,
    "org.scala-lang" % "scala-library" % Versions.scala,
    "org.scala-lang" % "scala-reflect" % Versions.scala,

    "org.apache.kafka" %% "kafka" % Versions.kafka,
    "org.apache.kafka" % "kafka-clients" % Versions.kafka,

    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-streaming" % Versions.spark,
    "org.apache.spark" %% "spark-sql" % Versions.spark,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % Versions.spark,
    "com.github.benfradet" %% "spark-kafka-0-10-writer" % "0.2.0",

    "org.slf4j" % "slf4j-log4j12" % "1.7.21",
    "org.slf4j" % "slf4j-api" % "1.7.21",

    "com.typesafe" % "config" % "1.2.1",
    "net.ceedubs" % "ficus_2.11" % "1.1.2",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )
/*
  val commonExcludeDependencies = Seq(
    "org.slf4j" % "slf4j-log4j12"
  )
*/
  lazy val main = Project(projectName, base = file("."))
    .settings(commonSettings: _*)
    //.settings(assemblySettings: _*)
    .settings(scalacOptions ++= commonScalacOptions)
    .settings(libraryDependencies ++= commonLibraryDependencies)
    //.settings(excludeDependencies ++= commonExcludeDependencies)
}

