name := "slender"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
//libraryDependencies += "com.github.nikita-volkov" % "sext" % "0.2.4"