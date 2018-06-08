//package slender
//
//import org.scalatest.FunSuite
//import org.apache.spark.sql.SparkSession
//import shapeless.syntax.std.tuple._
//
//class EvaluatorTest extends FunSuite {
//  import implicits._
//
////  val stringCounts1 = PhysicalCollection(
////    Map("a" -> 1, "b" -> 2, "c" -> 3)
////  )
////
////  val stringCounts2 = PhysicalCollection(
////    Map("a" -> 2, "b" -> 4, "c" -> 6)
////  )
////
////  val bagOfIntPairs = PhysicalCollection(
////    Map(
////      (1, 1) -> 1,
////      (1, 2) -> 2,
////      (2, 2) -> 3
////    )
////  )
////
////  val bagOfIntPairPairs = PhysicalCollection(
////    Map(
////      ((1,1),(1,1)) -> 1
////    )
////  )
//
//  test("Product ring test") {
//    val expr = ProductRingExpr((NumericExpr(1),NumericExpr(1)).productElements)
//    println(expr)
//  }
//
//}
