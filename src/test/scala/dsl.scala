package slender

import dsl._

import org.scalatest.FunSuite

object collections {
  val stringCounts1 = Collection(DomStringType, IntType, "stringCounts1")
  val stringCounts2 = Collection(DomStringType, IntType, "stringCounts2")
  val bagOfBags = Bag(BoxedRingType(MappingType(DomStringType, IntType)), "bagOfBags")
  val intCounts = Collection(DomIntType, IntType, "intCounts")
  val bagOfPairs = Bag(KeyPair(DomStringType,DomIntType), "bagOfPairs")
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
    assert(query.isResolved)
    assert(query.ringType == IntType)
    assert(query ==
      Sum(stringCounts1 * {"x" ==> 1}).resolve
    )
  }

  test("Simple yield") {
    val query = For ("x" <-- stringCounts1) Yield "x"
    assert(query.isResolved)
    assert(query.ringType == MappingType(DomStringType,IntType))
    assert(query ==
      Sum(stringCounts1 * {"x" ==> sng("x")}).resolve
    )
  }

  test("Predicated yield") {
    val query = For ("x" <-- intCounts iff "x" === 1) Yield "x"
    assert(query.isResolved)
    assert(query.ringType == MappingType(DomIntType,IntType))
    assert(query ==
      Sum(intCounts * {"x" ==> sng("x","x"===1)}).resolve
    )
  }

  test("Flatten") {
    val query = For ("x" <-- bagOfBags) Collect fromK("x")
    assert(query.isResolved)
    assert(query.ringType == BagType(DomStringType))
    assert(query ==
      Sum(bagOfBags * {"x" ==> fromK("x")}).resolve
    )
  }

  test ("Ring nesting") {
    val query =
      For ("k" <-- bagOfPairs) Yield (
        "k"._1 --> sng("k"._2)
      )
    assert(query.isResolved)
    assert(query.ringType == MappingType(DomStringType,BagType(DomIntType)))
  }

  test ("Key nesting") {
    val group: KeyExpr => RingExpr = k =>
      For ("k1" <-- bagOfPairs iff k === "k1"._1) Yield "k1"._2

    val query = For ("k" <-- bagOfPairs) Yield (
        ("k"._1, toK(group("k"._1)))
      )

    assert(query.isResolved)
    assert(query.ringType ==
      BagType(
        (DomStringType, box(BagType(DomIntType)))
      )
    )
  }


}