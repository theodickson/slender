package slender

import slender.execution._
import slender.execution.implicits._

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

}

class ExecutionTests extends FunSuite {

  val ctx = MyExecutionContext

  val stringCounts = (ctx.stringCounts, "stringCounts")

  val stringIntPairs = (ctx.stringIntPairs, "stringIntPairs")

  val stringIntNestedPairs = (ctx.stringIntNestedPairs, "stringIntNestedPairs")

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

}
