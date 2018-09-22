name := "slender"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)
libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.3"
)

scalacOptions in ThisBuild += "-Xlog-implicits"