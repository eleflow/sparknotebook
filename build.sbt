/*
* Copyright 2015 eleflow.com.br.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import sbt._
import sbt.Keys._
import sbt.complete.Parsers
import com.typesafe.sbt.SbtNativePackager._
import NativePackagerKeys._
import com.typesafe.sbt.SbtGit._

versionWithGit

organization := "eleflow"

name := "SparkNotebook"

version :="0.1.0"

resolvers += Resolver.mavenLocal

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "SparkNotebook Maven Repo" at "https://s3-us-west-2.amazonaws.com/sparknotebook-repo/release"

resolvers += "SparkNotebook Snapshot Maven Repo" at "https://s3-us-west-2.amazonaws.com/sparknotebook-repo/snapshot"

instrumentSettings

ScoverageKeys.highlighting := true

packageArchetype.java_application

testOptions in Test += Tests.Argument("-oDF")

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.8.9.1"
  , "org.apache.spark" % "spark-repl_2.10" % "1.2.0" % "provided"
  , "org.apache.spark" % "spark-sql_2.10" % "1.2.0" % "provided"
  , "org.apache.spark" % "spark-hive_2.10" % "1.2.0" % "provided"
  , "org.apache.spark" % "spark-hive-thriftserver_2.10" % "1.2.0" % "provided"
  , "org.scalatest" %% "scalatest" % "2.2.2" % "test->*"
  , "mysql" % "mysql-connector-java" % "5.1.34"
  , "joda-time" % "joda-time" % "2.5"
  , "com.scalatags" %% "scalatags" % "0.4.1"
  , "org.refptr.iscala" %% "iscala" % "0.3-SNAPSHOT" changing()
  , "com.gensler" %% "scalavro" % "0.6.2" exclude("org.slf4j", "slf4j-api") exclude("com.google.guava", "guava") exclude("ch.qos.logback", "logback-classic")
)

parallelExecution in Test := false

parallelExecution in ScoverageTest := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

def sparkNotebookVersion(version:Option[String] = Some("Not a Git Repository"), dir:File) = {
  val file = dir / "SparkNotebookVersion.scala"
  IO.write(file,
    s"""package eleflow.sparknotebook

      object SparkNotebookVersion{
          val version = "${version.get}"
      }
    """)
  Seq(file)
}

sourceGenerators in Compile <+= (git.gitHeadCommit,sourceManaged in Compile) map sparkNotebookVersion
