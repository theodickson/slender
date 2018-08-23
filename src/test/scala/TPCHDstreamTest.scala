package slender

import shapeless.syntax.singleton._
import java.lang.System.nanoTime

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

class TPCHDstreamTest extends SlenderSparkStreamingTest {

  import dsl._

  val batchDuration = Milliseconds(50)
  val sampleName = "1000_customers"
  val batchSizes = Seq(100,250)
//  val rdds = new TpchRdds(sampleName)
//  val dstreams = new TpchDstreams(sampleName, N)
//  val locals = new TpchLocal(sampleName)
//
//  lazy val customer = Bag(rdds.customer, "customer".narrow)
//  lazy val orders = Bag(rdds.orders, "orders".narrow)
//  lazy val lineitem = Bag(rdds.lineitem, "lineitem".narrow)
//  lazy val part = Bag(rdds.part, "part".narrow)
//  lazy val partSupp = Bag(rdds.partSupp, "partSupp".narrow)
//  lazy val supplier = Bag(rdds.supplier, "supplier".narrow)
//  lazy val nation = Bag(rdds.nation, "nation".narrow)
//  lazy val region = Bag(rdds.region, "region".narrow)
//
//  lazy val customerStreamed = Bag(dstreams.customer, "customer".narrow)
//  lazy val ordersStreamed = Bag(dstreams.orders, "orders".narrow)
//  lazy val lineitemStreamed = Bag(dstreams.lineitem, "lineitem".narrow)
//  lazy val partStreamed = Bag(dstreams.part, "part".narrow)
//  lazy val partSuppStreamed = Bag(dstreams.partSupp, "partSupp".narrow)
//  lazy val supplierStreamed = Bag(dstreams.supplier, "supplier".narrow)
//  lazy val nationStreamed = Bag(dstreams.nation, "nation".narrow)
//  lazy val regionStreamed = Bag(dstreams.region, "region".narrow)

  val (c_custkey,c_name,c_nationkey) = Vars("C1","C2","C3")
  val (o_orderkey,o_custkey,o_orderdate) = Vars("O1","O2","O3")
  val (s_suppkey,s_name,s_nationkey) = Vars("S1","S2","S3")
  val (l_orderkey,l_partkey,l_suppkey) = Vars("L1","L2","L3")
  val (p_partkey,p_name) = Vars("P1","P2")
  val (ps_partkey, ps_suppkey) = Vars("PS1","PS2")
  val (n_nationkey,n_regionkey,n_name) = Vars("N1","N2","N3")
  val (r_regionkey,r_name) = Vars("R1", "R2")


  def singleDStreamTest(test: (SparkSession,StreamingContext) => Unit): Unit = {
    val spark = SparkSession.builder
      .appName("test")
      .config("spark.master", "local[1]")
      .getOrCreate()

    val ssc = {
      val sc = spark.sparkContext
      val out = new StreamingContext(sc,batchDuration)
      out.checkpoint("_checkpoint")
      out
    }
    test(spark,ssc)
  }

//  test("Table skews") {
//    val spark = SparkSession.builder
//      .appName("test")
//      .config("spark.master", "local[1]")
//      .getOrCreate()
//    import spark.implicits._
//    val sc = spark.sparkContext
//    val rdds = new TpchRdds("10000_customers")(spark)
//    val customer = rdds.customer.toDF("c_custkey", "c_name", "c_nationkey")
//    customer.select("c_nationkey","c_custkey").distinct.groupBy("c_nationkey").count.describe("count").show
//
//    val lineitem = rdds.lineitem.toDF("l_orderkey","l_partkey","l_suppkey")
//    lineitem.select("l_orderkey","l_partkey").distinct.groupBy("l_orderkey").count.describe("count").show
//
//    val orders = rdds.orders.toDF("o_orderkey","o_custkey","o_orderdate")
//    orders.select("o_custkey","o_orderdate").distinct.groupBy("o_custkey").count.describe("count").show
//    spark.stop()
//  }
//
//  test("Group isolation test - lineitem") {
//    def testGen(sampleName: String,
//                batchSize: Int,
//                shredded: Boolean,
//                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
//      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
//      val rdds = new TpchRdds(sampleName)(spark)
//      val dstream = rddToDStream(rdds.lineitem, batchSize, ssc)
//      val lineitem = Bag(dstream, "lineitem".narrow)
//      val query = Group(
//        For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem) Yield (l_orderkey,l_partkey)
//      )
//
//      if (!shredded) query.eval.profile(batchSize,List(rdds.lineitem),spark,ssc)
//      else query.shreddedEval.profile(batchSize,List(rdds.lineitem),acc,spark,ssc)
//      println()
//    }
//    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
//    batchSizes.foreach { batchSize =>
//      tests += testGen(sampleName,batchSize,false,false)
//      tests += testGen(sampleName,batchSize,true,false)
//      tests += testGen(sampleName,batchSize,true,true)
//    }
//    println(s"Query - group(lineitem). Dataset - $sampleName")
//    tests.foreach(singleDStreamTest)
//    println()
//  }
//
//  test("Group isolation test - customer") {
//    def testGen(sampleName: String,
//                batchSize: Int,
//                shredded: Boolean,
//                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
//      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
//      val rdds = new TpchRdds(sampleName)(spark)
//      val dstream = rddToDStream(rdds.customer, batchSize, ssc)
//      val customer = Bag(dstream, "customer".narrow)
//      val query = Group(
//        For ((c_custkey,c_name,c_nationkey) <-- customer) Yield (c_nationkey,c_custkey)
//      )
//      if (!shredded) query.eval.profile(batchSize,List(rdds.lineitem),spark,ssc)
//      else query.shreddedEval.profile(batchSize,List(rdds.lineitem),acc,spark,ssc)
//      println()
//    }
//    val sampleName = "10000_customers"
//    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
//    val batchSizes = Seq(5,10,25,50)
//    batchSizes.foreach { batchSize =>
//      tests += testGen(sampleName,batchSize,false,false)
//      tests += testGen(sampleName,batchSize,true,false)
//      tests += testGen(sampleName,batchSize,true,true)
//    }
//    println(s"Query - group(customer). Dataset - $sampleName")
//    tests.foreach(singleDStreamTest)
//    println()
//  }
//
//  test("Group isolation test - orders") {
//    def testGen(sampleName: String,
//                batchSize: Int,
//                shredded: Boolean,
//                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
//      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
//      val rdds = new TpchRdds(sampleName)(spark)
//      val dstream = rddToDStream(rdds.orders, batchSize, ssc)
//      val orders = Bag(dstream, "orders".narrow)
//      val query = Group(
//        For ((o_orderkey,o_custkey,o_orderdate) <-- orders) Yield (o_custkey,o_orderdate)
//      )
//      if (!shredded) query.eval.profile(batchSize,List(rdds.lineitem),spark,ssc)
//      else query.shreddedEval.profile(batchSize,List(rdds.lineitem),acc,spark,ssc)
//      println()
//    }
//    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
//    batchSizes.foreach { batchSize =>
//      tests += testGen(sampleName,batchSize,false,false)
//      tests += testGen(sampleName,batchSize,true,false)
//      tests += testGen(sampleName,batchSize,true,true)
//    }
//    println(s"Query - group(orders). Dataset - $sampleName")
//    tests.foreach(singleDStreamTest)
//    println()
//  }

  test("Q1") {

    /**Q1 with ONLY lineitem streamed*/
    def genQuery[V,ID](rdds: TpchRdds, lineitem: LiteralExpr[V,ID]) = {
      val customer = Bag(rdds.customer, "customer".narrow)
      val orders = Bag(rdds.orders, "orders".narrow)
      val part = Bag(rdds.part, "part".narrow)

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

    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {

      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val inputs = List(rdds.lineitem, rdds.part, rdds.orders, rdds.customer)
      val dstream = rddToDStream(rdds.lineitem, batchSize, ssc)
      val lineitem = Bag(dstream, "lineitem".narrow)
      val query = genQuery(rdds, lineitem)
      if (!shredded) query.eval.profile(batchSize,inputs,spark,ssc)
      else query.shreddedEval.profile(batchSize,inputs,acc,spark,ssc)
      println()
    }
    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
    batchSizes.foreach { batchSize =>
      tests += testGen(sampleName,batchSize,false,false)
      tests += testGen(sampleName,batchSize,true,false)
      tests += testGen(sampleName,batchSize,true,true)
    }
    println(s"Query - Q1. Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }

//  test("Q2") {
//
//    /**Q2 with ONLY orders streamed*/
//    def genQuery[V,ID](rdds: TpchRdds, orders: LiteralExpr[V,ID]) = {
//      val custNames = Var("X")
//      val lineitem = Bag(rdds.lineitem, "lineitem".narrow)
//      val customer = Bag(rdds.customer, "customer".narrow)
//      val supplier = Bag(rdds.supplier, "supplier".narrow)
//
//      //First join orders with lineitems on orderkey to get all pairs of custkey/suppkey
//      val custSuppliers = For ((__,o_custkey,__,__,l_suppkey) <-- orders.join(lineitem)) Yield (o_custkey,l_suppkey)
//      //Then join customer with the above to get the customers names, yielding all pairs of suppkey/custname.
//      //Then group this to get each supplier with its bag of customer names who have used it:
//      val inner = For ((__,c_name,__,l_suppkey) <-- customer.join(custSuppliers)) Yield (l_suppkey,c_name)
//      val supplierCustomers = Group(
//        For ((__,c_name,__,l_suppkey) <-- customer.join(custSuppliers)) Yield (l_suppkey,c_name)
//      )
//      //Finally join with supplier to get the suppliers names:
//      val query = For ((__,s_name,__,custNames) <-- supplier.join(supplierCustomers)) Yield (s_name,custNames)
//      query
//    }
//
//    def testGen(sampleName: String,
//                batchSize: Int,
//                shredded: Boolean,
//                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
//
//      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
//      val rdds = new TpchRdds(sampleName)(spark)
//      val inputs = List(rdds.lineitem, rdds.supplier, rdds.orders, rdds.customer)
//      val dstream = rddToDStream(rdds.orders, batchSize, ssc)
//      val orders = Bag(dstream, "orders".narrow)
//      val query = genQuery(rdds, orders)
//      if (!shredded) query.eval.profile(batchSize,inputs,spark,ssc)
//      else query.shreddedEval.profile(batchSize,inputs,acc,spark,ssc)
//      println()
//    }
//    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
//    batchSizes.foreach { batchSize =>
//      tests += testGen(sampleName,batchSize,false,false)
//      tests += testGen(sampleName,batchSize,true,false)
//      tests += testGen(sampleName,batchSize,true,true)
//    }
//    println(s"Query - Q2. Dataset - $sampleName")
//    tests.foreach(singleDStreamTest)
//    println()
//  }
//
//  test("Q3") {
//
//    /**Q3 with ONLY partSupp streamed*/
//    def genQuery[V,ID](rdds: TpchRdds, partSupp: LiteralExpr[V,ID]) = {
//      /**For each part, return the name, the names and nations of all suppliers who supply it,
//      * and the names and nations of all customers who've ordered it
//      * */
//
//      val (suppliers,customers) = Vars("SUPS","CUSTS")
//      val lineitem = Bag(rdds.lineitem, "lineitem".narrow)
//      val customer = Bag(rdds.customer, "customer".narrow)
//      val supplier = Bag(rdds.supplier, "supplier".narrow)
//      val part = Bag(rdds.part, "part".narrow)
//      val orders = Bag(rdds.orders, "orders".narrow)
//
//      //First join partSupp to suppliers and group to get partkeys and their bags of supplier names
//      val partSupp2 = For ((ps_partkey,ps_suppkey) <-- partSupp) Yield (ps_suppkey,ps_partkey)
//      val partSuppliers = Group(
//        For ((ps_suppkey,ps_partkey,s_name,s_nationkey) <-- partSupp2.join(supplier)) Yield (ps_partkey,(s_name,s_nationkey))
//      )
//
//      val partCustomerPairs = For ((__,l_partkey,__,o_custkey,__) <-- lineitem.join(orders)) Yield (l_partkey,o_custkey)
//      val partCustomers = Group(
//        For ((l_partkey,__,c_name,c_nationkey) <-- partCustomerPairs.join(customer)) Yield (l_partkey,(c_name,c_nationkey))
//      )
//
//      val query =
//        For (
//          (__,p_name,suppliers,customers) <-- part.join(partSuppliers.join(partCustomers))
//        ) Yield (p_name,suppliers,customers)
//
//      query
//    }
//
//    def testGen(sampleName: String,
//                batchSize: Int,
//                shredded: Boolean,
//                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
//
//      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
//      val rdds = new TpchRdds(sampleName)(spark)
//      val inputs = List(rdds.lineitem, rdds.supplier, rdds.orders, rdds.customer, rdds.part, rdds.partSupp)
//      val dstream = rddToDStream(rdds.partSupp, batchSize, ssc)
//      val partSupp = Bag(dstream, "partSupp".narrow)
//      val query = genQuery(rdds, partSupp)
//      if (!shredded) query.eval.profile(batchSize,inputs,spark,ssc)
//      else query.shreddedEval.profile(batchSize,inputs,acc,spark,ssc)
//      println()
//    }
//    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
//    batchSizes.foreach { batchSize =>
//      tests += testGen(sampleName,batchSize,false,false)
//      tests += testGen(sampleName,batchSize,true,false)
//      tests += testGen(sampleName,batchSize,true,true)
//    }
//    println(s"Query - Q3. Dataset - $sampleName")
//    tests.foreach(singleDStreamTest)
//    println()
//  }
//
////  test("Q4") {
////
////    /**Q4 with ONLY orders streamed*/
////    def genQuery[V,ID](rdds: TpchRdds, orders: LiteralExpr[V,ID]) = {
////
////      val (orderKeys0,custNames0,nationCustNames) = Vars("X1", "X2","X3")
////      val nation = Bag(rdds.nation, "nation".narrow)
////      val region = Bag(rdds.region, "region".narrow)
////      val customer = Bag(rdds.customer, "customer".narrow)
////
////      val customerOrders = Group(
////        For ((o_orderkey,o_custkey,o_orderdate) <-- orders) Yield (o_custkey,o_orderkey)
////      )
////
////      val nationCustomers = Group(
////        For ((c_custkey,orderKeys0,c_name,c_nationkey) <-- customerOrders.join(customer)) Yield (c_nationkey,c_name,orderKeys0)
////      )
////
////      val regionCustomers = Group(
////        For((n_nationkey,n_regionkey,n_name,custNames0) <-- nation.join(nationCustomers)) Yield (n_regionkey,(n_name,custNames0))
////      )
////
////      val query = For ((r_regionkey,r_name,nationCustNames) <-- region.join(regionCustomers)) Yield (r_name,nationCustNames)
//////      region.join(regionCustomers)
//////
//////      val query = Group(
//////        For ((r_regionkey,r_name,nationCustNames) <-- region.join(regionCustomers)) Yield (r_name,nationCustNames)
//////      )
////      query
////
////      //regionCustomers
////
////    }
////
////    def testGen(sampleName: String,
////                batchSize: Int,
////                shredded: Boolean,
////                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
////
////      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
////      val rdds = new TpchRdds(sampleName)(spark)
////      val inputs = List(rdds.orders, rdds.nation, rdds.region, rdds.customer)
////      val dstream = rddToDStream(rdds.orders, batchSize, ssc)
////      val orders = Bag(dstream, "orders".narrow)
////      val query = genQuery(rdds, orders)
////      query.shreddedEval//.printType
//////      if (!shredded) query.eval.profile(batchSize,inputs,spark,ssc)
//////      else query.shreddedEval.profile(batchSize,inputs,acc,spark,ssc)
////      println()
////    }
////    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
////    batchSizes.foreach { batchSize =>
////      tests += testGen(sampleName,batchSize,false,false)
////      tests += testGen(sampleName,batchSize,true,false)
////      tests += testGen(sampleName,batchSize,true,true)
////    }
////    println(s"Query - Q4. Dataset - $sampleName")
////    tests.foreach(singleDStreamTest)
////    println()
////  }
//
//  test("Q5") {
//
//    /**Q5 with ONLY customer streamed*/
//    def genQuery[V,ID](rdds: TpchRdds, customer: LiteralExpr[V,ID]) = {
//
//      val (custName0,regionName0) = Vars("X1","X2")
//      val nation = Bag(rdds.nation, "nation".narrow)
//      val region = Bag(rdds.region, "region".narrow)
//
//      val regionNations = For ((n_nationkey,n_regionkey,n_name) <-- nation) Yield (n_regionkey,n_nationkey)
//
//      val regionNameNations = Collect(
//        For ((n_regionkey,n_nationkey,r_name) <-- regionNations.join(region)) Yield (n_nationkey,r_name)
//      )
//
//      val customerNations = For ((c_custkey,c_name,c_nationkey) <-- customer) Yield (c_nationkey,c_name)
//
//      val query = Group(
//          For ((__,custName0,regionName0) <-- customerNations.join(regionNameNations)) Yield (regionName0,custName0)
//      )
//      query
//    }
//
//    def testGen(sampleName: String,
//                batchSize: Int,
//                shredded: Boolean,
//                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
//
//      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
//      val rdds = new TpchRdds(sampleName)(spark)
//      val inputs = List(rdds.nation, rdds.region, rdds.customer)
//      val dstream = rddToDStream(rdds.customer, batchSize, ssc)
//      val customer = Bag(dstream, "customer".narrow)
//      val query = genQuery(rdds, customer)
//      if (!shredded) query.eval.profile(batchSize,inputs,spark,ssc)
//      else query.shreddedEval.profile(batchSize,inputs,acc,spark,ssc)
//      println()
//    }
//    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
//    batchSizes.foreach { batchSize =>
//      tests += testGen(sampleName,batchSize,false,false)
//      tests += testGen(sampleName,batchSize,true,false)
//      tests += testGen(sampleName,batchSize,true,true)
//    }
//    println(s"Query - Q5. Dataset - $sampleName")
//    tests.foreach(singleDStreamTest)
//    println()
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
