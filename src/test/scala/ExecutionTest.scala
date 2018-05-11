package slender


import slender.execution._
import slender.execution.implicits._
import slender.dsl.implicits._

import scala.reflect.runtime.universe._
import org.scalatest.FunSuite

object MyExecutionContext extends ToolboxExecutionContext {

  val stringCounts =
    Map(
      ("a",1),
      ("b",2),
      ("c",3)
    )

  val stringIntPairs =
    Map(
      ("a",(1,1)),
      ("b",(2,2)),
      ("c",(3,3))
    )

  val stringIntNestedPairs =
    Map(
      ("a",((1,1),(1,1))),
      ("b",((2,2),(2,2))),
      ("c",((3,3),(3,3)))
    )

  val nestedStringBag =
    Map(
      ("b", Map("b" -> 2)),
      ("c", Map("c" -> 2)),
      ("d", Map("d" -> 2))
    )

}

class ExecutionTests extends FunSuite {

  val ctx = MyExecutionContext

  import ctx._

  val executor = LocalExecutor(MyExecutionContext)

  test("Evaluate Int.") {
    val result = executor.execute(IntExpr(1))
    assert(result == 1)
  }

  test("Retrieve list collection unchanged.") {
    val result = executor.execute(stringCounts)
    assert(result ==
      Map(
        ("a",1),
        ("b",2),
        ("c",3)
      )
    )
  }

  test("Sum simple bag.") {
    val result = executor.execute(Sum(stringCounts))
    assert(result == 6)
  }

  test("Sum mapping to pairs.") {
    val result = executor.execute(Sum(stringIntPairs))
    assert(result == (6,6))
  }

  test("Sum mapping to nested pairs.") {
    val result = executor.execute(Sum(stringIntNestedPairs))
    assert(result == ((6,6),(6,6)))
  }

  test("Add two mappings, then sum") {
    val expr = Sum(Add(stringIntPairs,stringIntPairs))
    val result = executor.execute(expr)
    assert(result == (12,12)) //(2,2) + (4,4) + (6,6)
  }

  test("Simple dot test") {
    val expr = Dot(stringCounts, IntExpr(2))
    val result = executor.execute(expr)
    println(result)
    assert(result == Map(
      ("a",2),
      ("b",4),
      ("c",6)
    ))
  }

  test("Multiplication test") {
    val expr = Multiply(stringCounts, nestedStringBag)
    val result = executor.execute(expr)
    assert(result == Map(
      ("a", Map()),
      ("b", Map("b" -> 4)),
      ("c", Map("c" -> 6))
    ))
  }

  test("Dot test") {
    val expr = Dot(stringCounts,nestedStringBag)
    val result = executor.execute(expr)
    println(result)
  }

//  test("Inf mapping test") {
//    val expr = For ("x" <-- stringCounts) Collect 2
//    val result = executor.execute(expr)
//    println(result)
//  }

}
