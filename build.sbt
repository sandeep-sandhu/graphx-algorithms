name := "graphx-algorithms"

version := "0.1"

scalaVersion := "2.13.10"

enablePlugins(AssemblyPlugin)

assembly / mainClass := Some("SparkApp")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided"
  , "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
  , "org.apache.spark" %% "spark-streaming" % "3.4.0" % "provided"
  , "org.apache.spark" %% "spark-streaming-kafka-0-10"  % "3.4.0" % "provided"
  , "org.apache.spark" %% "spark-graphx"  % "3.4.0" % "provided"
  , "org.apache.spark" %% "spark-mllib"  % "3.4.0" % "provided"
  , "io.netty" % "netty-buffer" % "4.1.68.Final" % "provided"
  , "log4j" % "log4j" % "1.2.17" % "provided"
)

// for plotting in scala:
// https://mvnrepository.com/artifact/org.scalanlp/breeze-viz
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "1.2"  % "provided"
libraryDependencies += "org.jfree" % "jfreechart" % "1.5.4"


// for testing:
//coverageEnabled := true

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.15"


javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
//  test / fork := true

// config file reader:
libraryDependencies += "com.typesafe" % "config" % "1.4.2"

// for including the Postgresql Database JDBC driver:
// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.1"


scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  //removed in scala 2.13 "-Xfuture",
  //removed in scala 2.13 "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused"
)
