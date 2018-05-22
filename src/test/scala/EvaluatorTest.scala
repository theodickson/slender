package slender

import org.scalatest.FunSuite
import slender.algebra.implicits._

class EvaluatorTests extends FunSuite {

  val stringCounts1 = PhysicalCollection(
    Map("a" -> 1, "b" -> 2, "c" -> 3)
  )

  val stringCounts2 = PhysicalCollection(
    Map("a" -> 2, "b" -> 4, "c" -> 6)
  )

//  test("Dot test") {
//    val query = DotExpr(stringCounts1,stringCounts2)
//    val result = query.eval
//    println(result)
//  }
//
//  test("Nested dot test") {
//    val query = DotExpr(DotExpr(stringCounts1,stringCounts2),stringCounts1)
//    println(query.eval)
//  }
//
//  test("Add test") {
//    val query = AddExpr(stringCounts1,stringCounts2)
//    println(query.eval)
//  }

  test("Inf mapping test") {
    val query = SumExpr(
      MultiplyExpr( //[Map[String,Int],String => Int,Map[String,Int]]
        stringCounts1,
        InfiniteMappingExpr(
          TypedVariable[String]("x"),IntExpr(2)
        )
      )//(infMultiply[String,Int,Int,Int](IntDot))
    )
    println(query.eval)
  }
}
