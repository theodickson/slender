package slender

import org.scalatest.FunSuite
import shapeless._

class SparkEvaluatorTest extends SlenderSparkTest {
  import dsl._
  import spark.implicits._
//
//  test("Group test") {
//    val data = PhysicalCollection(
//      Map(
//        (1,1) -> 1,
//        (1,2) -> 2,
//        (2,1) -> 1
//      )
//    ).distribute
//    val query = Group(data)
//    val result = query.eval
//    result.rdd.toDF.show
////    assert(result == Map(
////      (1,Map(1 -> 1, 2 -> 2)) -> 1,
////      (2,Map(1 -> 1)) -> 1
////    ))
//  }
//
//  test("Shapeless rdd test") {
//    val data = List(1::HNil, 2::HNil)
//    sc.parallelize(data).take(2).foreach(println)
//  }
//  test("Join test") {
//    val rdd1 = sc.parallelize {
//      Map(
//        (1::"1"::HNil) -> 1,
//        (1::"2"::HNil) -> 2
//      ).toSeq
//    }
//    val data1 = PhysicalCollection(toPairRDD(rdd1))
//
//    val rdd2 = sc.parallelize {
//      Map(
//        (1::true::HNil) -> 3,
//        (2::false::HNil) -> 2
//      ).toSeq
//    }
//    val data2 = PhysicalCollection(toPairRDD(rdd2))
//
//    data1.join(data2).eval.collect.foreach(println)
//  }
//
}
