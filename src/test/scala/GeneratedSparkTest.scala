package slender

import org.scalatest.FunSuite
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SparkSession

import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

class GeneratedSparkTest extends FunSuite {

  val classLoader = getClass.getClassLoader
  val mirror = runtimeMirror(classLoader)
  val tb = mirror.mkToolBox()
//  val tb2 = runtimeMirror(c
//  println(tb.mirror.classLoader)

  def eval(code: Tree) = tb.eval(code)

//  test("Spark") {
//    val spark = SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
//    import spark.implicits._
//    List(1,2,3).toDF
//    spark.stop
//  }

  test("Spark generated") {
    val code =
      q"""
        import org.apache.spark.sql.SparkSession
        val sp = SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
        import sp.implicits._
        val dataset = List(1,2,3,4,5).toDS
        dataset.show
       """
    eval(code)
  }

}
