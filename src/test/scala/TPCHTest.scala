package slender

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TPCHLocalTest extends FunSuite {

  import implicits._
  import TestUtils._

  implicit val (spark,sc) = TestUtils.getSparkSession

  val maps = new TpchLocal("10_customers")

  lazy val customer = mapToPhysicalCollection(maps.customer)
  lazy val orders = mapToPhysicalCollection(maps.orders)
  lazy val lineitem = mapToPhysicalCollection(maps.lineitem)
  lazy val part = mapToPhysicalCollection(maps.part)
  lazy val partSupp = mapToPhysicalCollection(maps.partSupp)
  lazy val supplier = mapToPhysicalCollection(maps.supplier)
  lazy val nation = mapToPhysicalCollection(maps.nation)
  lazy val region = mapToPhysicalCollection(maps.region)

  val (c_custkey,c_name,c_nationkey) = (C1,C2,C3)
  val (o_orderkey,o_custkey,o_orderdate) = (O1,O2,O3)
  val (s_suppkey,s_name,s_nationkey) = (S1,S2,S3)
  val (l_orderkey,l_partkey,l_suppkey) = (Y1,Y2,Y3)
  val (p_partkey,p_name) = (P1,P2)

  test("Q1") {
    /** For each customer, return their name, and for each date on which they've ordered,
      * return the date and all the part names they ordered on that date.
      */
//    val q =
//      For ((c_custkey,c_name,c_nationkey) <-- customer) Yield
//        (c_name,
//          For ((o_orderkey,o_custkey,o_orderdate) <-- orders iff (c_custkey === o_custkey)) Yield
//            (o_orderdate,
//              For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem iff (l_orderkey === o_orderkey)) Collect
//                (For ((p_partkey,p_name) <-- part iff (l_partkey === p_partkey)) Yield p_name)
//      )
//    )


    val q =
      For ((c_custkey,c_name,c_nationkey) <-- customer) Yield
        (c_name,
          For ((o_orderkey,o_custkey,o_orderdate) <-- orders iff (c_custkey === o_custkey)) Yield
            (o_orderdate,
              For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem iff (l_orderkey === o_orderkey),
                   (p_partkey,p_name) <-- part iff (l_partkey === p_partkey))
              Yield p_name
            )
        )

    assert(q.isResolved)
    assert(q.isEvaluable)
    println(q.evalType)
//    println(q.eval)
  }

  test("Q2") {
    /**For each supplier, return the name and the names of all customers who have used them*/
    val q =
      For ((s_suppkey,s_name,s_nationkey) <-- supplier) Yield
        (s_name,
          For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem iff (s_suppkey === l_suppkey)) Collect
            (
              For ((o_orderkey,o_custkey,o_orderdate) <-- orders iff (l_orderkey === o_orderkey)) Collect (
                For ((c_custkey,c_name,c_nationkey) <-- customer iff (o_custkey === c_custkey)) Yield c_name
              )
            )
        )
    assert(q.isResolved)
    assert(q.isEvaluable)
    println(q.evalType)
//    println(q.eval)
  }

}
