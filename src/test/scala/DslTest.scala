//package slender
//
////import slender.dsl.implicits._
//import slender.definitions._
//import org.scalatest.FunSuite
//
//object collections {
//  val stringCounts1 = PhysicalCollection(StringKeyType, IntType, "stringCounts1")
//  val stringCounts2 = PhysicalCollection(StringKeyType, IntType, "stringCounts2")
//  val bagOfBags = PhysicalBag(BoxedRingType(FiniteMappingType(StringKeyType, IntType)), "bagOfBags")
//  val intCounts = PhysicalCollection(IntKeyType, IntType, "intCounts")
//  val bagOfPairs = PhysicalBag(ProductKeyType(StringKeyType,IntKeyType), "bagOfPairs")
//  val const = IntExpr(1)
//}
//
//class DslTests extends FunSuite {
//
//  import slender.dsl.implicits._
//  import collections._
//
//  test("Operators") {
//    assert(
//      stringCounts1 + stringCounts2 == Add(stringCounts1,stringCounts2) &&
//      -Sum(stringCounts1) == Negate(Sum(stringCounts1)) &&
//      stringCounts1 * stringCounts2 == Multiply(stringCounts1,stringCounts2) &&
//      (stringCounts1 dot stringCounts2) == Dot(stringCounts1, stringCounts2) &&
//      !Sum(stringCounts1) == Not(Sum(stringCounts1))
//    )
//  }
//
//  test("Simple for-comprehension") {
//    val query = For ("x" <-- stringCounts1) Collect 1
//    assert(query.isTyped)
//    assert(query.exprType == IntType)
//    assert(query ==
//      Sum(stringCounts1 * {"x" ==> 1}).inferTypes
//    )
//  }
//
//  test("Simple yield") {
//    val query = For ("x" <-- stringCounts1) Yield "x"
//    assert(query.isTyped)
//    assert(query.exprType == FiniteMappingType(StringKeyType,IntType))
//    assert(query ==
//      Sum(stringCounts1 * {"x" ==> sng("x")}).inferTypes
//    )
//  }
//
//  test("Predicated yield") {
//    val query = For ("x" <-- intCounts iff "x" === 1) Yield "x"
//    assert(query.isTyped)
//    assert(query.exprType == FiniteMappingType(IntKeyType,IntType))
//    assert(query ==
//      Sum(intCounts * {"x" ==> sng("x","x"===1)}).inferTypes
//    )
//  }
//
//  test("Flatten") {
//    val query = For ("x" <-- bagOfBags) Collect fromK("x")
//    assert(query.isTyped)
//    assert(query.exprType == BagType(StringKeyType))
//    assert(query ==
//      Sum(bagOfBags * {"x" ==> fromK("x")}).inferTypes
//    )
//  }
//
////  test ("Ring nesting") {
////    val query =
////      For ("k" <-- bagOfPairs) Yield (
////        "k"._1 --> sng("k"._2)
////      )
////    assert(query.isTyped)
////    assert(query.exprType == FiniteMappingType(StringKeyType,BagType(IntKeyType)))
////  }
//
//  test ("Ring nesting 2") {
//    val query =
//      For (("k1","k2") <-- bagOfPairs) Yield (
//        "k1" --> sng("k2")
//      )
//    assert(query.isTyped)
//    assert(query.exprType == FiniteMappingType(StringKeyType,BagType(IntKeyType)))
//  }
//
////  test ("Key nesting") {
////    val group: KeyExpr => RingExpr = k =>
////      For ("k1" <-- bagOfPairs iff k === "k1"._1) Yield "k1"._2
////
////    val query = For ("k" <-- bagOfPairs) Yield (
////        ("k"._1, toK(group("k"._1)))
////      )
////
////    assert(query.isTyped)
////    assert(query.exprType ==
////      BagType(
////        (StringKeyType, BoxedRingType(BagType(IntKeyType)))
////      )
////    )
////  }
//
//  test("Make key expr test") {
//    val query = For ("x" <-- intCounts iff "x" === 1) Yield ( ("x","x") )
//    println(query.explain)
//    assert(query.isTyped)
//    assert(query.exprType == FiniteMappingType((IntKeyType,IntKeyType),IntType))
//    assert(query ==
//      Sum(intCounts * {"x" ==> sng(("x","x"),"x"===1)}).inferTypes
//    )
//  }
//
//  test ("Key nesting 2") {
//    val group: KeyExpr => RingExpr = k =>
//      For (("k11","k22") <-- bagOfPairs iff k === "k11") Yield "k22"
//
//    val query = For (("k1","k2") <-- bagOfPairs) Yield (
//      ("k1", toK(group("k1")))
//    )
//
//    assert(query.isTyped)
//    assert(query.exprType ==
//      BagType(
//        (StringKeyType, BoxedRingType(BagType(IntKeyType)))
//      )
//    )
//  }
//
//  test("Untyped query") {
//    val query = For ("x" <-- bagOfBags) Yield (
//      For ("y" <-- "x") Yield (
//        "y" --> sng("z")
//      )
//    )
//    assert(!query.isTyped)
//    val typedQuery = query.inferTypes(Map("z" -> StringKeyType))
//    assert(typedQuery.isTyped)
//  }
//}