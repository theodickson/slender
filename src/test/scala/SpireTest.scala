//package slender
//
//import org.scalatest.FunSuite
//import spire.implicits._
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SparkSession
//
//import scala.tools.reflect.ToolBox
//import scala.reflect.runtime.universe._
//import scala.reflect.runtime.currentMirror
//
//class SpireTests extends FunSuite {
//
//  val tb = currentMirror.mkToolBox()
//
//  def eval(code: Tree) = tb.eval(code)
//
//
////  test("Ring +") {
////    println(Map(1 -> 2) + Map(1 -> 2))
////    println(eval(q"""Map(1 -> 2) + Map(1 -> 2)"""))
////  }
//
////  test("Spark") {
////    val spark = SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
////    import spark.implicits._
////    List(1,2,3).toDF
////    spark.stop
////  }
//
//  test("Spark generated") {
//    val code =
//      q"""
//        import com.twitter.algebird.implicits._
//        import org.apache.spark.sql.SparkSession
//        val spark = SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
//        import spark.implicits._
//        val df = List(1,2,3).toDF("a")
//        df.show
//        spark.stop
//       """
//    eval(code)
//  }
////
////  test("Import spire") {
////    val code =
////      q"""
////         import spire.implicits._
////         val x = List(1,2,3)
////       """
////  }
//}