package slender

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TPCHSparkTest extends FunSuite {

  import dsl._
  import TestUtils._

  implicit val (spark,sc) = TestUtils.getSparkSession

  val rdds = new TpchRdds("10_customers")

  lazy val customer = rddToPhysicalBag(rdds.customer)
  lazy val orders = rddToPhysicalBag(rdds.orders)
  lazy val lineitem = rddToPhysicalBag(rdds.lineitem)
  lazy val part = rddToPhysicalBag(rdds.part)
  lazy val partSupp = rddToPhysicalBag(rdds.partSupp)
  lazy val supplier = rddToPhysicalBag(rdds.supplier)
  lazy val nation = rddToPhysicalBag(rdds.nation)
  lazy val region = rddToPhysicalBag(rdds.region)

  val (c_custkey,c_name,c_nationkey) = (C1,C2,C3)
  val (o_orderkey,o_custkey,o_orderdate) = (O1,O2,O3)
  val (s_suppkey,s_name,s_nationkey) = (S1,S2,S3)
  val (l_orderkey,l_partkey,l_suppkey) = (Y1,Y2,Y3)
  val (p_partkey,p_name) = (P1,P2)
  val (orderkey,partname) = (X1,X2)
  val ((custkey,orderdate),partname1) = ((X,Y),Z)
  val ((orderdate1,partname2),custname) = ((W1,W2),W3)

    test("Q1") {
      /** For each customer, return their name, and for each date on which they've ordered,
        * return the date and all the part names they ordered on that date.
        */
      //Map each partkey to the bag of orderkeys which contain it, yielding a partkey --> Bag[orderkey]
      val partOrders = For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem) Yield l_partkey --> sng(l_orderkey)
      //Turn parts into a partkey --> Bag[partname] mapping (each bag will contain just one element, but we need it like this for the join)
      val partNames = For ((p_partkey,p_name) <-- part) Yield p_partkey --> sng(p_name)
//      //Multiply partOrders and partNames, to get a partkey --> Bag[orderkey x partname], then sum, to get just a Bag[orderkey x partname],
//      //which contains every orderkey,partname pair. The outer for comprehension turns this into an orderkey --> Bag[partname] mapping.
      val orderParts = For ((orderkey,partname) <-- sum(partOrders * partNames)) Yield orderkey --> sng(partname)
//      //Create an orderkey --> Bag[custkey x orderdate] mapping from the orders table.
      val orderCustomers =
        For ((o_orderkey,o_custkey,o_orderdate) <-- orders) Yield o_orderkey --> sng((o_custkey,o_orderdate))
//      Using a similar pattern to the above join, multiply the orderParts with the orderCustomers and sum to get a
//      Bag[custkey x orderdate x partname] which contains all such triples. Then we use a For/Yield to turn this into a
//      custkey --> Bag[orderdate x partname] mapping.
      val customerKeyOrders =
        For (((custkey,orderdate),partname1) <-- sum(orderCustomers * orderParts)) Yield custkey --> sng((orderdate,partname1))
      //Create a custkey --> Bag[custname] mapping from the customer table.
      val customerNames = For ((c_custkey,c_name,c_nationkey) <-- customer) Yield c_custkey --> sng(c_name)
      //Join this with the customerKeyOrders and sum to get a Bag[custname x orderdate x parname]
      //Then use a For/Yield to reshape this as a custname --> (orderdate --> partname).
      val customerOrders =
        For ((custname,(orderdate1,partname2)) <-- sum(customerNames * customerKeyOrders)) Yield
          custname --> sng(orderdate1,sng(partname2))

      customerOrders.resolve
//      _assert(customerOrders.isResolved)
//      _assert(customerOrders.isEvaluable)
//      println(customerOrders.evalType)
//      customerOrders.eval.collect.foreach(println)
    }
  //
  //  test("Q2") {
  //    /**For each supplier, return the name and the names of all customers who have used them*/
  //    val q =
  //      For ((s_suppkey,s_name,s_nationkey) <-- supplier) Yield
  //        (s_name,
  //          For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem iff (s_suppkey === l_suppkey)) Collect
  //            (
  //              For ((o_orderkey,o_custkey,o_orderdate) <-- orders iff (l_orderkey === o_orderkey)) Collect (
  //                For ((c_custkey,c_name,c_nationkey) <-- customer iff (o_custkey === c_custkey)) Yield c_name
  //              )
  //            )
  //        )
  //    assert(q.isResolved)
  //    assert(q.isEvaluable)
  //    println(q.evalType)
  ////    println(q.eval)
  //  }

}
