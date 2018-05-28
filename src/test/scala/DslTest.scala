package slender

import org.scalatest.FunSuite

//object collections {
////  val stringCounts1 = PhysicalCollection(StringKeyType, IntType, "stringCounts1")
////  val stringCounts2 = PhysicalCollection(StringKeyType, IntType, "stringCounts2")
////  val bagOfBags = PhysicalBag(BoxedRingType(FiniteMappingType(StringKeyType, IntType)), "bagOfBags")
////  val intCounts = PhysicalCollection(IntKeyType, IntType, "intCounts")
////  val bagOfPairs = PhysicalBag(ProductKeyType(StringKeyType,IntKeyType), "bagOfPairs")
////  val const = IntExpr(1)
//}
//
class DslTests extends FunSuite {

  import implicits._

  val stringCounts1 = PhysicalCollection(
    Map("a" -> 1, "b" -> 2, "c" -> 3)
  )

  val stringCounts2 = PhysicalCollection(
    Map("a" -> 2, "b" -> 4, "c" -> 6)
  )

  val bagOfIntPairs = PhysicalCollection(
    Map(
      (1, 1) -> 1,
      (1, 2) -> 2,
      (2, 2) -> 3
    )
  )

  val bagOfIntPairPairs = PhysicalCollection(
    Map(
      ((1,1),(1,1)) -> 1
    )
  )

  test("") {
    import implicits._

//    val query1 = For (X <-- stringCounts1) Yield (X,
//      For (X <-- stringCounts1) Yield (
//        (X, For (X <-- stringCounts1) Yield X)
//      )
//    )
//    println(query1.eval)
//
//    val query = For ((X,Y) <-- bagOfIntPairs) Yield (
//      (X, For ((Z,W) <-- bagOfIntPairs) Yield (
//        (Z, For ((X1,Y1) <-- bagOfIntPairs) Yield (
//          (Y, For ((W1,W1) <-- bagOfIntPairs) Yield Z)
//        ))
//        )
//      )
//    )
//    println(query.eval)
  }

}