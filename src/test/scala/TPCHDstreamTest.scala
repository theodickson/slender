package slender

import java.sql.Timestamp

import org.apache.spark.streaming.StreamingContext
import shapeless._
import shapeless.ops.hlist.Replacer
import shapeless.syntax.singleton._

class TPCHDstreamTest extends SlenderSparkStreamingTest {

  import dsl._
  import spark.implicits._

  val rdds = new TpchRdds("10_customers")
  val dstreams = new TpchDstreams("10_customers", N)

  lazy val customer = Bag(rdds.customer, "customer".narrow)
  lazy val orders = Bag(rdds.orders, "orders".narrow)
  lazy val lineitem = Bag(rdds.lineitem, "lineitem".narrow)
  lazy val part = Bag(rdds.part, "part".narrow)
  lazy val partSupp = Bag(rdds.partSupp, "partSupp".narrow)
  lazy val supplier = Bag(rdds.supplier, "supplier".narrow)
  lazy val nation = Bag(rdds.nation, "nation".narrow)
  lazy val region = Bag(rdds.region, "region".narrow)

  lazy val customerStreamed = Bag(dstreams.customer, "customer".narrow)
  lazy val ordersStreamed = Bag(dstreams.orders, "orders".narrow)
  lazy val lineitemStreamed = Bag(dstreams.lineitem, "lineitem".narrow)
  lazy val partStreamed = Bag(dstreams.part, "part".narrow)
  lazy val partSuppStreamed = Bag(dstreams.partSupp, "partSupp".narrow)
  lazy val supplierStreamed = Bag(dstreams.supplier, "supplier".narrow)
  lazy val nationStreamed = Bag(dstreams.nation, "nation".narrow)
  lazy val regionStreamed = Bag(dstreams.region, "region".narrow)

  val (c_custkey,c_name,c_nationkey) = Vars("C1","C2","C3")
  val (o_orderkey,o_custkey,o_orderdate) = Vars("O1","O2","O3")
  val (s_suppkey,s_name,s_nationkey) = Vars("S1","S2","S3")
  val (l_orderkey,l_partkey,l_suppkey) = Vars("L1","L2","L3")
  val (p_partkey,p_name) = Vars("P1","P2")
  val (ps_partkey, ps_suppkey) = Vars("PS1","PS2")


  object Q1 {
    /**Q1 with ONLY lineitem streamed*/
    def apply[V,ID](lineitem: LiteralExpr[V,ID]) = {
      val (orderKey0, partName0) = Vars("X1", "X2")
      val (partNames0, custKey0, orderDate0) = Vars("Y1", "Y2", "Y3")
      val (orderDate1, partNames1, custName0) = Vars("Z1", "Z2", "Z3")

      //Drop the suppkeys to get a Bag[(partkey,orderkey)]
      val partOrders = For((l_orderkey, l_partkey, __) <-- lineitem) Yield(l_partkey, l_orderkey)

      //join with part to get a Bag[(partkey,(orderKey,partName))], drop the partkeys and group to get
      //a Bag[(orderKey,Bag[partName])]
      val orderPartNames = Group(
        For((__, orderKey0, partName0) <-- partOrders.join(part)) Yield(orderKey0, partName0)
      )
      //join with orders and reshape to get a Bag[(custKey,orderDate,Bag[partName])]
      val customerOrders =
        For((__, partNames0, custKey0, orderDate0) <-- orderPartNames.join(orders)) Yield(custKey0, orderDate0, partNames0)

      //join with customers and reshape to get a Bag[(custName,orderDate,Bag[partName])]
      val customerNameOrders =
        For((__, orderDate1, partNames1, custName0, __) <-- customerOrders.join(customer)) Yield
          (custName0, orderDate1, partNames1)

      //finally group to get a Bag[(custName,Bag[(orderDate,Bag[partName])])]
      val query = Group(customerNameOrders)
      query
    }
  }

  test("Q1") {
    val static = Q1(lineitem)
    val streamed = Q1(lineitemStreamed)
    val staticResult = static.eval
    val streamedResult = streamed.eval
    staticResult.printData
    streamedResult.print
    dstreamEqualsRdd(streamedResult.dstream, staticResult).print
//    streamed.eval.print
//    streamed.foreachRdd
    ssc.start
    ssc.awaitTermination()
    ssc.stop(true)
  }

  test("Q1-shredded-inc") {
    val query = Q1(lineitemStreamed)
    val ShreddedResult(flat,ctx) = query.shreddedEval
    //flat.printType; ctx.printType
    flat.print
    ctx.head.print
    ctx.tail.head.print
    ssc.start
    ssc.awaitTermination()
    ssc.stop(true)
  }

  test("Q1-shredded-acc") {
    val query = Q1(lineitemStreamed)
    val ShreddedResult(flat,ctx) = query.shreddedEval
    //flat.printType; ctx.printType
    flat.acc.print
    ctx.head.acc.print
    ctx.tail.head.acc.print
    ssc.start
    ssc.awaitTermination()
    ssc.stop(true)
  }
//  object Q2 {
//    /**For each supplier, return the name and the names of all customers who have used them*/
//
//    val custNames = Var("X")
//    //First join orders with lineitems on orderkey to get all pairs of custkey/suppkey
//    val custSuppliers = For ((__,o_custkey,__,__,l_suppkey) <-- orders.join(lineitem)) Yield (o_custkey,l_suppkey)
//    //Then join customer with the above to get the customers names, yielding all pairs of suppkey/custname.
//    //Then group this to get each supplier with its bag of customer names who have used it:
//    val inner = For ((__,c_name,__,l_suppkey) <-- customer.join(custSuppliers)) Yield (l_suppkey,c_name)
//    val supplierCustomers = Group(
//      For ((__,c_name,__,l_suppkey) <-- customer.join(custSuppliers)) Yield (l_suppkey,c_name)
//    )
//    //Finally join with supplier to get the suppliers names:
//    val query = For ((__,s_name,__,custNames) <-- supplier.join(supplierCustomers)) Yield (s_name,custNames)
//  }
//
//  test("Q2") {
//    import Q2._
//    query.shreddedReport("Q2")
//  }
//
//  object Q3 {
//    /**For each part, return the name, the names and nations of all suppliers who supply it,
//      * and the names and nations of all customers who've ordered it
//      * */
//
//    val (suppliers,customers) = Vars("SUPS","CUSTS")
//    //First join partSupp to suppliers and group to get partkeys and their bags of supplier names
//    val partSupp2 = For ((ps_partkey,ps_suppkey) <-- partSupp) Yield (ps_suppkey,ps_partkey)
//    val partSuppliers = Group(
//      For ((ps_suppkey,ps_partkey,s_name,s_nationkey) <-- partSupp2.join(supplier)) Yield (ps_partkey,(s_name,s_nationkey))
//    )
//
//    val partCustomerPairs = For ((__,l_partkey,__,o_custkey,__) <-- lineitem.join(orders)) Yield (l_partkey,o_custkey)
//    val partCustomers = Group(
//      For ((l_partkey,__,c_name,c_nationkey) <-- partCustomerPairs.join(customer)) Yield (l_partkey,(c_name,c_nationkey))
//    )
//
//    val query =
//      For (
//        (__,p_name,suppliers,customers) <-- part.join(partSuppliers.join(partCustomers))
//      ) Yield (p_name,suppliers,customers)
//
//  }
//
//  test("Q3") {
//    import Q3._
//    query.shreddedReport("Q3")
//  }

  //  object Q4 {
  //    /**
  //      * For ((c_name, c_orders)) <-- Q1 //c_name: String, c_orders: Map[(timestamp,Map[string,int]),Int]
  //      * (o_orderdate,o_parts) <-- c_orders //o_orderdate: Timestamp, o_parts: Map[String,Int]
  //      * (p_name,l_qty) <-- o_parts) //p_name: String, l_qty: Int
  //      * yield ((c_name,p_name,getMonth(o_orderdate)),l_qty) //(custName,partName,month),qty
  //      * .reduceByKey(_ + _)
  //      *
  //      * So this is:
  //      * Yield all triples of customername,partname,month, with the value for each the number of times they ordered
  //      * that part in that month.
  //      * NB. we cannot do this the way the it is in the paper and also do shredded evaluation as we cannot do lookups.
  //      * However, if we do it directly from the tables we never create any inner collections so shredded evaluation would
  //      * be no different to normal evaluation anyway.
  //      *
  //      *
  //      */
  //
  //    val (c_name,c_orders,o_parts) = Vars("W1","W2","W3")
  //    val getMonth = (t: Timestamp) => t.toLocalDateTime.getMonth.toString
  //
  //    val query =
  //      For ((c_name,c_orders) <-- Q1.query) Collect ( //LiteralExpr(Q1.query.shreddedEval.nested,"Q1")
  //        For ((o_orderdate,o_parts) <-- c_orders) Collect (
  //          For (p_name <-- o_parts) Yield (c_name,p_name,getMonth $ o_orderdate)
  //        )
  //      )
  //  }
  //
  //  test("Q4") {
  //    import Q4._
  //    query.report("Q4")
  //  }
  //
  //  object Q5 {
  //    /**
  //      * for ((p_name,suppliers,customers) <-- Q3; //p_name: String, suppliers: Map[(String,Int),Int]
  //      *     (c_name,c_nationkey) <-- customers; //c_name: String, c_nationkey: Int
  //      *     if suppliers.forall { case (_,s_nationkey) => c_nationkey != s_nationkey }
  //      *     //if for all suppliers of the part, they are from a different nation to the customer:
  //      *     //yield the part name
  //      *     yield (p_name,1) (reduceByKey)
  //      *
  //      *  First note that this requires the suppliers and customers to be bags of name,nationkey pairs not just
  //      *  names which is how Q3 is actually written.
  //      *
  //      *  What this does is, for each part in the output of Q3, iterate over the customers, for each customer which is
  //      *  in a different nation to every supplier for that part, yield the partname.
  //      *  The output is a simple bag of part names, but the values represent then number of times it was ordered
  //      *  by a customer not from a nation which has a supplier for that part.
  //      *
  //      *  Again as this uses fromK (in order to access the boxed collections of suppliers and customers created in Q3)
  //      *  it cannot be shreddedEval'd as it stands.
  //      *
  //      *
  //      */
  //
  //    val (supps,custs) = Vars("S","C")
  //    val f = (c_nationkey: Int, suppliers: Map[String::Int::HNil,Int]) =>
  //      suppliers.forall { case ((_::s_nationkey::HNil),_) => c_nationkey != s_nationkey }
  //
  //    val query =
  //      For ((p_name,supps,custs) <-- Q3.query) Collect (
  //        For (((c_name,c_nationkey) <-- custs) If f.$(c_nationkey,supps)) Yield p_name
  //        )
  //  }
  //
  //  test("Q5") {
  //    import Q5._
  //    query.report("Q5")
  //  }

  //  test("Q6") {
  //    /**
  //      * For ((c_name,_) <-- Customer //for each customer
  //      * Yield (c_name, //yield the customer's name,
  //      * for ((s_name,customers2) <-- Q2; //and for each supplier_name and bag of customer names from Q2
  //      *      (c_name2 <- customers2 if c_name2 == c_name) //for each customer in the bag matching the top-most custmoer
  //      *      yield s-name //yield the supplier name
  //      *
  //      * So this creates a Bag[(custname,[Bag[supplierName]])] which is just a reversal of Q2
  //      *
  //      * Again this uses fromK and also it does nested iteration....
  //      */
  //  }
  //
  //  test("Q7") {
  //    /**
  //      * For (n <- Nation)
  //      * Yield (n.name,
  //      *        for ((p_name,suppliers,customers) <-- Q3
  //      *        if suppliers.exists (equals nationkey) &&
  //      *           customers.forall (notequal nationkey)
  //      *           yield p_name)
  //      *
  //      * So this creates a Bag[(nationName,Bag[partName])] where the parts are all those which
  //      * have a supplier in that nation but had no customers in that nation.
  //      * fromK, nested iteration here too.
  //      */
  //  }



  //    test("Q1 value-nested") {
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
}
