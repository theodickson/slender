package slender

import slender.dsl.implicits._
import slender.dsl.inference._
import org.scalatest.FunSuite

object collections {
  val stringCounts1 = PhysicalCollection(StringKeyType, IntType, "stringCounts1")
  val stringCounts2 = PhysicalCollection(StringKeyType, IntType, "stringCounts2")
  val bagOfBags = PhysicalBag(BoxedRingType(MappingType(StringKeyType, IntType)), "bagOfBags")
  val intCounts = PhysicalCollection(IntKeyType, IntType, "intCounts")
  val bagOfPairs = PhysicalBag(KeyPairType(StringKeyType,IntKeyType), "bagOfPairs")
  val const = IntExpr(1)
}

class DslTests extends FunSuite {

  import collections._

  test("Operators") {
    assert(
      stringCounts1 + stringCounts2 == Add(stringCounts1,stringCounts2) &&
      -Sum(stringCounts1) == Negate(Sum(stringCounts1)) &&
      stringCounts1 * stringCounts2 == Multiply(stringCounts1,stringCounts2) &&
      (stringCounts1 dot stringCounts2) == Dot(stringCounts1, stringCounts2) &&
      !Sum(stringCounts1) == Not(Sum(stringCounts1))
    )
  }

  test("Simple for-comprehension") {
    val query = For ("x" <-- stringCounts1) Collect 1
    assert(query.isTyped)
    assert(query.ringType == IntType)
    assert(query ==
      inferTypes(Sum(stringCounts1 * {"x" ==> 1}))
    )
  }

  test("Simple yield") {
    val query = For ("x" <-- stringCounts1) Yield "x"
    assert(query.isTyped)
    assert(query.ringType == MappingType(StringKeyType,IntType))
    assert(query ==
      inferTypes(Sum(stringCounts1 * {"x" ==> sng("x")}))
    )
  }

  test("Predicated yield") {
    val query = For ("x" <-- intCounts iff "x" === 1) Yield "x"
    assert(query.isTyped)
    assert(query.ringType == MappingType(IntKeyType,IntType))
    assert(query ==
      inferTypes(Sum(intCounts * {"x" ==> sng("x","x"===1)}))
    )
  }

  test("Flatten") {
    val query = For ("x" <-- bagOfBags) Collect fromK("x")
    assert(query.isTyped)
    assert(query.ringType == BagType(StringKeyType))
    assert(query ==
      inferTypes(Sum(bagOfBags * {"x" ==> fromK("x")}))
    )
  }

  test ("Ring nesting") {
    val query =
      For ("k" <-- bagOfPairs) Yield (
        "k"._1 --> sng("k"._2)
      )
    assert(query.isTyped)
    assert(query.ringType == MappingType(StringKeyType,BagType(IntKeyType)))
  }

  test ("Key nesting") {
    val group: KeyExpr => RingExpr = k =>
      For ("k1" <-- bagOfPairs iff k === "k1"._1) Yield "k1"._2

    val query = For ("k" <-- bagOfPairs) Yield (
        ("k"._1, toK(group("k"._1)))
      )

    assert(query.isTyped)
    assert(query.ringType ==
      BagType(
        (StringKeyType, BoxedRingType(BagType(IntKeyType)))
      )
    )
  }

  test("Unresolved query") {
    val query = For ("x" <-- bagOfBags) Yield (
      For ("y" <-- "x") Yield (
        "y" --> sng("z")
      )
    )
    assert(!query.isTyped)
    val resolvedQuery = inferTypesR(query, Map("z" -> StringKeyType))
    assert(resolvedQuery.isTyped)
    println(resolvedQuery.ringType)
  }

}