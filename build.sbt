name := "slender"

version := "0.1"

scalaVersion := "2.11.12"

val tsecV = "0.0.1-M11"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)
//libraryDependencies += "org.typelevel" %% "spire" % "0.14.1"
//libraryDependencies += "com.twitter" %% "algebird-core" % "0.13.4"