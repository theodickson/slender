package slender

import dsl._

import org.scalatest.FunSuite

class DslTests extends FunSuite {

  val stringCounts1 = Collection(DomStringType, IntType, "stringCounts1")
  val stringCounts2 = Collection(DomStringType, IntType, "stringCounts2")
  val bagOfBags = Bag(BoxedRingType(MappingType(DomStringType, IntType)), "bagOfBags")
  val intCounts = Collection(DomIntType, IntType, "intCount")
  val bagOfPairs = Bag(KeyPair(DomStringType,DomStringType), "bagOfPairs")
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

//  test("Simple yield works") {
//      val q = For ("x" <-- bagOfBags) Yield (
//        "x"
//      )
//      println(q)
//      println(q.ringType)
//  }
//
//  test("flatten works") {
//    val q = For ("x" <-- bagOfBags) Collect "x"
//    println(q)
//    println(q.ringType)
//  }

  test ("nest works") {
    val q =
      For ("ks" <-- bagOfPairs) Yield (
        v"ks"._1 -> sng(v"ks"._2)
      )
    println(q)
    println(q.ringType)
  }


}