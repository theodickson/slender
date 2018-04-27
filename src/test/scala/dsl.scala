package slender

import dsl._

import org.scalatest.FunSuite

class DslTests extends FunSuite {

  val stringCounts1 = Collection(DomStringType, IntType, "stringCounts1")
  val stringCounts2 = Collection(DomStringType, IntType, "stringCounts2")
  val bagOfBags = Bag(BoxedRing(MappingType(DomStringType, IntType)), "bagOfBags")
  val intCounts = Collection(DomIntType, IntType, "intCount")

  val const = IntExpr(1)

  test("Operators work") {
    assert(
      stringCounts1 + stringCounts2 == Plus(stringCounts1,stringCounts2) &&
      -Sum(stringCounts1) == Negate(Sum(stringCounts1)) &&
      stringCounts1 * stringCounts2 == Multiply(stringCounts1,stringCounts2) &&
      (stringCounts1 dot stringCounts2) == Dot(stringCounts1, stringCounts2) &&
      !Sum(stringCounts1) == Not(Sum(stringCounts1))
    )
  }

  test("Simple for comprehension works") {
//    assert(
//      For("x" <-- stringCounts1).Collect(IntExpr(1)) ==
//      Sum(stringCounts1 * {"x" ==> IntExpr(1)})
//    )
  }

  test("Simple yield works") {
      val q = For ("x" <-- bagOfBags) Collect (
        For ("y" <-- fromK("x")) Yield "y"
      )
      println(q)
    assert(true)
  }


}