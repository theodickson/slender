package slender

class LocalEvaluatorTest extends SlenderTest {
  import dsl._

//  test("Group test") {
//    val data = PhysicalCollection(
//      Map(
//        (1,1) -> 1,
//        (1,2) -> 2,
//        (2,1) -> 1
//      )
//    )
//    val query = Group(data)
//    val result = query.eval
//    assert(result == Map(
//      (1,Map(1 -> 1, 2 -> 2)) -> 1,
//      (2,Map(1 -> 1)) -> 1
//    ))
//  }

  test("Product eval test") {
    val query = toExpr((IntKeyExpr(1),IntKeyExpr(1)))
    println(query)
    println(query.eval)
  }

}
