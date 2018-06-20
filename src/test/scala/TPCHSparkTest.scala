package slender

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TPCHSparkTest extends SlenderSparkTest {

  import dsl._

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
  val (l_orderkey,l_partkey,l_suppkey) = (L1,L2,L3)
  val (p_partkey,p_name) = (P1,P2)


    test("Q1 key-nested") {
      val (partKey0,(orderKey0,partName0)) = (X1,(X2,X3))
      val (partKey1,(partNames0,custKey0,orderDate0)) = (Y1,(Y2,Y3,Y4))
      val (custKey1,(orderDate1,partNames1,custName0,nationKey0)) = (Z1,(Z2,Z3,Z4,Z5))

      //Drop the suppkeys to get a Bag[(partkey,orderkey)]
      val partOrders = For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem) Yield (l_partkey,l_orderkey)
      //join with part to get a Bag[(partkey,(orderKey,partName))], drop the partkeys and group to get
      //a Bag[(orderKey,Bag[partName])]
      val orderPartNames = Group(
        For ((partKey0,(orderKey0,partName0)) <-- partOrders.join(part)) Yield (orderKey0,partName0)
      )
      //join with orders and reshape to get a Bag[(custKey,orderDate,Bag[partName])]
      val customerOrders =
        For ((partKey1,(partNames0,(custKey0,orderDate0))) <-- orderPartNames.join(orders)) Yield (custKey0,orderDate0,partNames0)

      //join with customers and reshape to get a Bag[(custName,orderDate,Bag[partName])]
      val customerNameOrders =
        For ((custKey1,((orderDate1,partNames1),(custName0,nationKey0))) <-- customerOrders.join(customer)) Yield
          (custName0,orderDate1,partNames1)

      //finally group to get a Bag[(custName,Bag[(orderDate,Bag[partName])])]
      val query = Group(customerNameOrders)

      query.resolve
      println(query.evalType)
      query.eval.take(10).foreach(println)

    }

//    test("Q1") {
//      /** For each customer, return their name, and for each date on which they've ordered,
//        * return the date and all the part names they ordered on that date.
//        */
//
//      val (orderkey,partname) = (X1,X2)
//      val ((custkey,orderdate),partname1) = ((X,Y),Z)
//      val ((orderdate1,partname2),custname) = ((W1,W2),W3)
//
//      //Map each partkey to the bag of orderkeys which contain it, yielding a partkey --> Bag[orderkey]
//      val partOrders = For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem) Yield l_partkey --> Sng(l_orderkey)
//      //Turn parts into a partkey --> Bag[partname] mapping (each bag will contain just one element, but we need it like this for the join)
//      val partNames = For ((p_partkey,p_name) <-- part) Yield p_partkey --> Sng(p_name)
////      //Multiply partOrders and partNames, to get a partkey --> Bag[orderkey x partname], then sum, to get just a Bag[orderkey x partname],
////      //which contains every orderkey,partname pair. The outer for comprehension turns this into an orderkey --> Bag[partname] mapping.
//      val orderParts = For ((orderkey,partname) <-- Sum(partOrders * partNames)) Yield orderkey --> Sng(partname)
////      //Create an orderkey --> Bag[custkey x orderdate] mapping from the orders table.
//      val orderCustomers =
//        For ((o_orderkey,o_custkey,o_orderdate) <-- orders) Yield o_orderkey --> Sng((o_custkey,o_orderdate))
////      Using a similar pattern to the above join, multiply the orderParts with the orderCustomers and sum to get a
////      Bag[custkey x orderdate x partname] which contains all such triples. Then we use a For/Yield to turn this into a
////      custkey --> Bag[orderdate x partname] mapping.
//      val customerKeyOrders =
//        For (((custkey,orderdate),partname1) <-- Sum(orderCustomers * orderParts)) Yield custkey --> Sng((orderdate,partname1))
//      //Create a custkey --> Bag[custname] mapping from the customer table.
//      val customerNames = For ((c_custkey,c_name,c_nationkey) <-- customer) Yield c_custkey --> Sng(c_name)
//      //Join this with the customerKeyOrders and sum to get a Bag[custname x orderdate x parname]
//      //Then use a For/Yield to reshape this as a custname --> (orderdate --> partname).
//      val customerOrders =
//        For ((custname,(orderdate1,partname2)) <-- Sum(customerNames * customerKeyOrders)) Yield
//          custname --> Sng(orderdate1,Sng(partname2))
//
//      customerOrders.resolve
////      _assert(customerOrders.isResolved)
////      _assert(customerOrders.isEvaluable)
////      println(customerOrders.evalType)
////      customerOrders.eval.collect.foreach(println)
//    }

//  test("Q2") {
//    /**For each supplier, return the name and the names of all customers who have used them*/
//    val q =
//      For ((s_suppkey,s_name,s_nationkey) <-- supplier) Yield
//        (s_name,
//          For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem Iff (s_suppkey === l_suppkey)) Collect
//            (
//              For ((o_orderkey,o_custkey,o_orderdate) <-- orders Iff (l_orderkey === o_orderkey)) Collect (
//                For ((c_custkey,c_name,c_nationkey) <-- customer Iff (o_custkey === c_custkey)) Yield c_name
//              )
//            )
//        )
//    assert(q.isResolved)
//    assert(q.isEvaluable)
//    println(q.evalType)
//    println(q.eval)
//  }

}
