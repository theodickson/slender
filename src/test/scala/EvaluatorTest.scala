package slender

import org.scalatest.FunSuite
import slender.algebra.implicits._
import org.apache.spark.sql.SparkSession
import slender.algebra.{Dot, PairRDD}


class EvaluatorTests extends FunSuite {

//  val spark = SparkSession.builder
//    .appName("test")
//    .config("spark.master", "local")
//    .getOrCreate()
//
//  implicit val sc = spark.sparkContext

  val stringCounts1 = PhysicalCollection(
    Map("a" -> 1, "b" -> 2, "c" -> 3)
  )

  val stringCounts2 = PhysicalCollection(
    Map("a" -> 2, "b" -> 4, "c" -> 6)
  )

//  val bagOfIntPairs = PhysicalCollection(
//      Map(
//        (1,1) -> 1,
//        (1,2) -> 2,
//        (2,2) -> 3
//      )
//  )

//  val stringCountsRdd = PhysicalCollection(PairRDD(sc.parallelize(List("a" -> 1, "b" -> 2, "c" -> 3))))

  test("Dot test") {
//    val query = SelfDotExpr(stringCounts1,stringCounts2)(productDots[String,String,Int,Int,Int](IntDot))
//    val query2 = SelfDotExpr2[Map[String,Int],PhysicalCollection[Map,String,Int],Map[(String,String),Int]](stringCounts1,stringCounts2)
    val query3 = SelfDotExpr3(stringCounts1,stringCounts2)

//    val query22 = SelfDotExpr2(stringCounts1,stringCounts2)
//    val query3 = SelfDotExpr2(IntExpr(1),IntExpr(1))
//    query3
//    que
//    (stringCounts1,stringCounts2)()
//    val result = query.eval
//    val result2 = query2.eval
//    println(result)
//    println(result2)
      //val result3 = query3.eval
    //println(result3)
//    query2
  }

//  test("Ev test") {
//    def go(implicit ev: IntExpr <:< Expr[Int]): Int = 1
//    def go2(implicit ev: PhysicalCollection2 <:< Expr[Map[String,Int]]): Int = 1
//    go; go2
//  }

//  test("Nested dot test") {
//    val query = DotExpr(DotExpr(stringCounts1,stringCounts2),stringCounts1)
//    println(query.eval)
//  }
//
//  test("Add test") {
//    val query = AddExpr(stringCounts1,stringCounts2)
//    println(query.eval)
//  }
//
//  test("Inf mapping test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        stringCounts1,
//        InfiniteMappingExpr(
//          Variable[String]("x"),IntExpr(2)
//        )
//      )
//    )
//    println(query.eval)
//  }
//
//  test("Rdd*inf mapping test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        stringCountsRdd,
//        InfiniteMappingExpr(
//          Variable[String]("x"),IntExpr(2)
//        )
//      )
//    )
//    println(query.eval)
//  }
//
//  test("Rdd*rdd test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        stringCountsRdd,
//        stringCountsRdd
//      )
//    )
//    println(query.eval)
//  }
//
//  test("Rdd*map test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        stringCountsRdd,
//        stringCounts1
//      )
//    )
//    println(query.eval)
//  }
////
//  test("Shredding test") {
//    val query = SumExpr(
//      MultiplyExpr(
//        bagOfIntPairs,
//        InfiniteMappingExpr(
//          Variable[(Int, Int)]("x"),
//          Sng(
//            BoxedRingExpr(
//              MultiplyExpr(
//                bagOfIntPairs,
//                InfiniteMappingExpr(
//                  Variable[(Int, Int)]("y"),
//                  Sng(Variable[(Int, Int)]("y"), IntExpr(1))
//                )
//              )
//            ), IntExpr(1)
//          )
//        )
//      )
//    )
//    println(query.eval)
////    println(query.shred.eval)
//  }

}
