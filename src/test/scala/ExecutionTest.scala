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

  val bagOfPairs =
    Map(
      ("a",1) -> 4,
      ("b",2) -> 3,
      ("b",3) -> 2,
      ("b",4) -> 1
    )

  val bagOfTriples =
    Map(
      (1,1,1) -> 1,
      (1,1,2) -> 2,
      (1,2,2) -> 3
    )

}

class ExecutionTests extends FunSuite {

  val ctx = MyExecutionContext

  import ctx._

  val interpreter = LocalInterpreter(MyExecutionContext)
//
//  test("Evaluate Int.") {
//    val result = interpreter.apply(IntExpr(1))
//    assert(result == 1)
//  }
//
//  test("Retrieve list collection unchanged.") {
//    val result = interpreter.apply(stringCounts)
//    assert(result ==
//      Map(
//        ("a",1),
//        ("b",2),
//        ("c",3)
//      )
//    )
//  }
//
//  test("Sum simple bag.") {
//    val result = interpreter.apply(Sum(stringCounts))
//    assert(result == 6)
//  }
//
//  test("Sum mapping to pairs.") {
//    val result = interpreter.apply(Sum(stringIntPairs))
//    assert(result == (6,6))
//  }
//
//  test("Sum mapping to nested pairs.") {
//    val result = interpreter.apply(Sum(stringIntNestedPairs))
//    assert(result == ((6,6),(6,6)))
//  }
//
//  test("Add two mappings, then sum") {
//    val expr = Sum(Add(stringIntPairs,stringIntPairs))
//    val result = interpreter.apply(expr)
//    assert(result == (12,12)) //(2,2) + (4,4) + (6,6)
//  }
//
//  test("Simple dot test") {
//    val expr = Dot(stringCounts, IntExpr(2))
//    val result = interpreter.apply(expr)
//    assert(result == Map(
//      ("a",2),
//      ("b",4),
//      ("c",6)
//    ))
//  }
//
//  test("Multiplication test") {
//    val expr = Multiply(stringCounts, nestedStringBag)
//    val result = interpreter.apply(expr)
//    assert(result == Map(
//      ("a", Map()), //todo
//      ("b", Map("b" -> 4)),
//      ("c", Map("c" -> 6))
//    ))
//  }
//
//  test("Dot test") {
//    val expr = Dot(stringCounts,nestedStringBag)
//    val result = interpreter.apply(expr)
//  }
//
//  test("Inf mapping simple test") {
//    val expr = For ("x" <-- stringCounts) Collect 2
//    val result = interpreter(expr)
//    assert(result == 12)
//  }
//
//  test("Inf mapping nested test") {
//    val expr = For ("x" <-- nestedStringBag) Collect 3
//    val result = interpreter(expr)
//    assert(result == Map("b" -> 6, "c" -> 6, "d" -> 6))
//  }
//
//  test("Nested tuple yield test") {
//    val expr = For ("x" <-- stringIntNestedPairs) Yield "x"
//    val result = interpreter(expr)
//    assert(result == stringIntNestedPairs)
//  }
//
//  test("Map-valued yield test") {
//    val expr = For ("x" <-- nestedStringBag) Yield "x"
//    val result = interpreter(expr)
////    println(expr.explain)
////    println(result)
//  }
//
//  test("Filtering test") {
//    val query = For ("x" <-- stringCounts iff "x" === StringKeyExpr("a")) Yield "x"
//    val result = interpreter(query)
//    assert(result == Map("a" -> 1, "b" -> 0, "c" -> 0))
//  }
//
//  test("Key-nesting test") {
//    val group: KeyExpr => RingExpr = k =>
//      For ("k1" <-- bagOfPairs iff "k1"._1 === k && "k1"._2 > 2) Yield "k1"._2
//    val query = For ("k" <-- bagOfPairs) Yield (
//      ("k"._1, toK(group("k"._1)))
//      )
//    println(query.explain)
//    val result = interpreter(query)
//    println(interpreter.showProgram(query))
//    println(result)
//  }
//
//  test("Group test") {
//    val group: KeyExpr => RingExpr = k =>
//      For ("k1" <-- bagOfPairs iff k === "k1"._1) Yield "k1"._2
//    val query = toK(group(StringKeyExpr("a")))
//    println(query.explain)
//    val result = interpreter(query)
//    println(interpreter.showProgram(query))
//    println(result)
//  }

//  test("Predef test") {
//    val group: KeyExpr => RingExpr = k =>
//      For ("k1" <-- bagOfPairs iff "k1"._1 === k) Yield "k1"._2
//    val query = For ("k" <-- bagOfPairs) Yield (
//      ("k"._1, toK(group("k"._1)))
//      )
//    val shredded = query.shred
//    val predef = interpreter.codeGen.predef(shredded)
//    interpreter.executor(q"..$predef")
//  }
//
//  test("Label init test") {
//    val group: KeyExpr => RingExpr = k =>
//      For ("k1" <-- bagOfPairs iff "k1"._1 === k) Yield "k1"._2
//    val query = For ("k" <-- bagOfPairs) Yield (
//      ("k"._1, toK(group("k"._1)))
//      )
//    val shredded = query.shred
//    val predef = interpreter.codeGen.predef(shredded)
//    interpreter.executor(
//      q"""..$predef
//         val label = Label100(("a",1))
//       """
//    )
//  }
//  test("Shredded key-nesting test") {
//    val group: KeyExpr => RingExpr = k =>
//      For ("k1" <-- bagOfPairs iff "k1"._1 === k) Yield "k1"._2
//    val query = For ("k" <-- bagOfPairs) Yield (
//      ("k"._1, toK(group("k"._1)))
//      )
//    val shredded = query.shred
//    println(shredded.explain)
//    println(interpreter.showProgram(shredded))
//    val result = interpreter(shredded)
//    println(result)
//  }

//  test("Renested key-nesting test") {
//    val group: KeyExpr => RingExpr = k =>
//      For ("k1" <-- bagOfPairs iff "k1"._1 === k) Yield "k1"._2
//    val query = For ("k" <-- bagOfPairs) Yield (
//      ("k"._1, toK(group("k"._1)))
//      )
//    val shredded = query.shred
//    val unshredded = shredded.unshred
//    println(unshredded.explain)
//    println(interpreter.showProgram(unshredded))
//    val result = interpreter(unshredded)
//    println(result)
//  }

  test("Double key-nesting test") {
    val query =
      For ("x" <-- bagOfTriples) Yield
        ("x"._1, toK(
          For ("y" <-- bagOfTriples iff "y"._1 === "x"._1) Yield
            ("y"._2, toK(
              For ("z" <-- bagOfTriples iff "z"._2 === "y"._2) Yield "z"._3
            )
            )
        )
        )
    val result = interpreter(query)
    println(interpreter.showProgram(query))
    println(interpreter.showProgram(query.shred))
    println(interpreter.showProgram(query.shred.renest))
//    val shreddedResult = interpreter(query.shred.renest)
//    val shreddedProgram = interpreter.showProgram(query.shred.renest)
//    println(query.shred.explain)
//    println(shreddedProgram)
    println(interpreter(query))
    println(interpreter(query.shred))
    println(interpreter(query.shred.renest))
  }


}
