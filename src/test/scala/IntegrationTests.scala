package slender

import org.scalatest.FunSuite

class IntegrationTests extends FunSuite with TestUtils {

  import implicits._

  test("Packet query") {
    val server = PhysicalCollection(
      Map(
        (1, Map((1,1,"1")->1), Map((1,1,"1")->1)) -> 1
      )
    )
    val query = For ((X,Y,Z) <-- server) Yield (
      (X, For (W <-- (toRing(Y) dot toRing(Z))) Yield W)
    )
    printType(server)
    printType(query)
    println(query.isResolved)
    println(query.eval)

//    query.resolve
    //println(query.eval)
//
//    val expr = PhysicalCollection(Map((1,1,"1")->1))
//    toRing(expr).eval
  }
}
