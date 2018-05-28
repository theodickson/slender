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
    val query = For ((X,Y,Z) <-- server) Collect fromK(Y)
    printType(server)
    printType(query)
    println(query.eval)
  }
}
