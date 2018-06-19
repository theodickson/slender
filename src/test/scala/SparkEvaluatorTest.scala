package slender

import org.scalatest.FunSuite

class SparkEvaluatorTest extends SlenderSparkTest {
  import dsl._
  import spark.implicits._

  test("Group test") {
    val data = PhysicalCollection(
      Map(
        (1,1) -> 1,
        (1,2) -> 2,
        (2,1) -> 1
      )
    ).distribute
    val query = Group(data)
    val result = query.eval
    result.rdd.toDF.show
//    assert(result == Map(
//      (1,Map(1 -> 1, 2 -> 2)) -> 1,
//      (2,Map(1 -> 1)) -> 1
//    ))
  }

  test("Join test") {
    val data1 = PhysicalCollection(
      Map(
        (1,"1") -> 1,
        (1,"2") -> 2
      )
    ).distribute

    val data2 = PhysicalCollection(
      Map(
        (1,true) -> 3,
        (2,false) -> 2
      )
    ).distribute

    val query = data1.join(data2)

    val result = query.eval

    result.show

  }

}
