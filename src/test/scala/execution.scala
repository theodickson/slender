package slender

import slender._
import slender.execution._
import org.scalatest.FunSuite
//
//class ExecutionTests extends FunSuite {
//
//  val stringCountsExpr = PhysicalBag(StringKeyType, "stringCounts")
//  val sumExpr = Sum(stringCountsExpr)
//
//  val stringCounts = {
//    val list = List(
//      ("a",1),
//      ("b",2),
//      ("c",3)
//    )
//    ListBag[String](list)
//  }
//
//  val ctx = ListExecutionContext("stringCounts" -> stringCounts)
//
//  test("Retrieve list from context unchanged.") {
//    val result = ListExecutor.execute(stringCountsExpr, ctx)
//    println(result)
//  }
//
//  test("Sum list.") {
//    val result = ListExecutor.execute(sumExpr, ctx)
//    println(result)
//  }
//
//
//}
