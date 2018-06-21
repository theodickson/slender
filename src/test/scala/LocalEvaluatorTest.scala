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

  test("Var product test") {
    val queryRaw = (X1,(X2,X3))
    def getQuery[T, V <: VariableExpr](t: T)(implicit make: MakeExpr[T,V]): V = make(t)
    val query = getQuery(queryRaw)
    printType(query)
  }

}
