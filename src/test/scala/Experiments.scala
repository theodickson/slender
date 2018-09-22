package slender

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import shapeless.syntax.singleton._

class TPCHDstreamTest extends SlenderSparkStreamingTest {

  import dsl._

  val batchDuration = Milliseconds(50)
  val sampleName = "10000_customers"
  val batchSizes = Seq(5,10,25)

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
      .config("spark.master", "local[2]")
      .getOrCreate()

    val ssc = {
      val sc = spark.sparkContext
      val out = new StreamingContext(sc,batchDuration)
      out.checkpoint("_checkpoint")
      out
    }
    test(spark,ssc)
  }


  test("Group isolation test - lineitem") {
    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val dstream = rddToDStream(rdds.lineitem, batchSize, ssc)
      val lineitem = Bag(dstream, "lineitem".narrow)
      val query = Group(
        For ((l_orderkey,l_partkey,l_suppkey) <-- lineitem) Yield (l_orderkey,l_partkey)
      )
      if (!shredded) query.eval.profile(batchSize,List(rdds.lineitem),spark,ssc)
      else query.shreddedEval.profile(batchSize,List(rdds.lineitem),acc,spark,ssc)
      println()
    }
    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
    batchSizes.foreach { batchSize =>
      tests += testGen(sampleName,batchSize,false,false)
      tests += testGen(sampleName,batchSize,true,false)
      tests += testGen(sampleName,batchSize,true,true)
    }
    println(s"Query - group(lineitem). Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }

  test("Group isolation test - part") {
    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val dstream = rddToDStream(rdds.part, batchSize, ssc)
      val part = Bag(dstream, "part".narrow)
      val query = Group(
        For ((p_partkey,p_name) <-- part) Yield (p_name,p_partkey)
      )
      if (!shredded) query.eval.profile(batchSize,List(rdds.part),spark,ssc)
      else query.shreddedEval.profile(batchSize,List(rdds.part),acc,spark,ssc)
      println()
    }
    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
    batchSizes.foreach { batchSize =>
      tests += testGen(sampleName,batchSize,false,false)
      tests += testGen(sampleName,batchSize,true,false)
      tests += testGen(sampleName,batchSize,true,true)
    }
    println(s"Query - group(part). Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }

  test("Group isolation test - partSupp") {
    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val dstream = rddToDStream(rdds.partSupp, batchSize, ssc)
      val partSupp = Bag(dstream, "partSupp".narrow)
      val query = Group(partSupp)
      if (!shredded) query.eval.profile(batchSize,List(rdds.partSupp),spark,ssc)
      else query.shreddedEval.profile(batchSize,List(rdds.partSupp),acc,spark,ssc)
      println()
    }
    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
    batchSizes.foreach { batchSize =>
      tests += testGen(sampleName,batchSize,false,false)
      tests += testGen(sampleName,batchSize,true,false)
      tests += testGen(sampleName,batchSize,true,true)
    }
    println(s"Query - group(partSupp). Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }

  test("Group isolation test - customer") {
    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val dstream = rddToDStream(rdds.customer, batchSize, ssc)
      val customer = Bag(dstream, "customer".narrow)
      val query = Group(
        For ((c_custkey,c_name,c_nationkey) <-- customer) Yield (c_nationkey,c_custkey)
      )
      if (!shredded) query.eval.profile(batchSize,List(rdds.lineitem),spark,ssc)
      else query.shreddedEval.profile(batchSize,List(rdds.lineitem),acc,spark,ssc)
      println()
    }
    val sampleName = "10000_customers"
    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
    val batchSizes = Seq(5,10,25,50)
    batchSizes.foreach { batchSize =>
      tests += testGen(sampleName,batchSize,false,false)
      tests += testGen(sampleName,batchSize,true,false)
      tests += testGen(sampleName,batchSize,true,true)
    }
    println(s"Query - group(customer). Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }

  test("Group isolation test - orders") {
    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {
      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val dstream = rddToDStream(rdds.orders, batchSize, ssc)
      val orders = Bag(dstream, "orders".narrow)
      val query = Group(
        For ((o_orderkey,o_custkey,o_orderdate) <-- orders) Yield (o_custkey,o_orderdate)
      )
      if (!shredded) query.eval.profile(batchSize,List(rdds.lineitem),spark,ssc)
      else query.shreddedEval.profile(batchSize,List(rdds.lineitem),acc,spark,ssc)
      println()
    }
    val tests = new collection.mutable.ArrayBuffer[DStreamTest]
    batchSizes.foreach { batchSize =>
      tests += testGen(sampleName,batchSize,false,false)
      tests += testGen(sampleName,batchSize,true,false)
      tests += testGen(sampleName,batchSize,true,true)
    }
    println(s"Query - group(orders). Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }

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

  test("Q2") {

    /**Q2 with ONLY orders streamed*/
    def genQuery[V,ID](rdds: TpchRdds, orders: LiteralExpr[V,ID]) = {
      val custNames = Var("X")
      val lineitem = Bag(rdds.lineitem, "lineitem".narrow)
      val customer = Bag(rdds.customer, "customer".narrow)
      val supplier = Bag(rdds.supplier, "supplier".narrow)

      //First join orders with lineitems on orderkey to get all pairs of custkey/suppkey
      val custSuppliers = For ((__,o_custkey,__,__,l_suppkey) <-- orders.join(lineitem)) Yield (o_custkey,l_suppkey)
      //Then join customer with the above to get the customers names, yielding all pairs of suppkey/custname.
      //Then group this to get each supplier with its bag of customer names who have used it:
      val inner = For ((__,c_name,__,l_suppkey) <-- customer.join(custSuppliers)) Yield (l_suppkey,c_name)
      val supplierCustomers = Group(
        For ((__,c_name,__,l_suppkey) <-- customer.join(custSuppliers)) Yield (l_suppkey,c_name)
      )
      //Finally join with supplier to get the suppliers names:
      val query = For ((__,s_name,__,custNames) <-- supplier.join(supplierCustomers)) Yield (s_name,custNames)
      query
    }

    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {

      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val inputs = List(rdds.lineitem, rdds.supplier, rdds.orders, rdds.customer)
      val dstream = rddToDStream(rdds.orders, batchSize, ssc)
      val orders = Bag(dstream, "orders".narrow)
      val query = genQuery(rdds, orders)
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
    println(s"Query - Q2. Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }

  test("Q3") {

    /**Q3 with ONLY partSupp streamed*/
    def genQuery[V,ID](rdds: TpchRdds, partSupp: LiteralExpr[V,ID]) = {
      /**For each part, return the name, the names and nations of all suppliers who supply it,
      * and the names and nations of all customers who've ordered it
      * */

      val (suppliers,customers) = Vars("SUPS","CUSTS")
      val lineitem = Bag(rdds.lineitem, "lineitem".narrow)
      val customer = Bag(rdds.customer, "customer".narrow)
      val supplier = Bag(rdds.supplier, "supplier".narrow)
      val part = Bag(rdds.part, "part".narrow)
      val orders = Bag(rdds.orders, "orders".narrow)

      val partSupp2 = For ((ps_partkey,ps_suppkey) <-- partSupp) Yield (ps_suppkey,ps_partkey)
      val partSuppliers = Group(
        For ((ps_suppkey,ps_partkey,s_name,s_nationkey) <-- partSupp2.join(supplier)) Yield (ps_partkey,(s_name,s_nationkey))
      )

      val partCustomerPairs = For ((__,l_partkey,__,o_custkey,__) <-- lineitem.join(orders)) Yield (l_partkey,o_custkey)
      val partCustomers = Group(
        For ((l_partkey,__,c_name,c_nationkey) <-- partCustomerPairs.join(customer)) Yield (l_partkey,(c_name,c_nationkey))
      )

      val query =
        For (
          (__,p_name,suppliers,customers) <-- part.join(partSuppliers.join(partCustomers))
        ) Yield (p_name,suppliers,customers)

      query
    }

    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {

      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val inputs = List(rdds.lineitem, rdds.supplier, rdds.orders, rdds.customer, rdds.part, rdds.partSupp)
      val dstream = rddToDStream(rdds.partSupp, batchSize, ssc)
      val partSupp = Bag(dstream, "partSupp".narrow)
      val query = genQuery(rdds, partSupp)
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
    println(s"Query - Q3. Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }

  test("Q4") {

    /**Q4 with ONLY customer streamed*/
    def genQuery[V,ID](rdds: TpchRdds, customer: LiteralExpr[V,ID]) = {

      val (custName0,regionName0) = Vars("X1","X2")
      val nation = Bag(rdds.nation, "nation".narrow)
      val region = Bag(rdds.region, "region".narrow)

      val regionNations = For ((n_nationkey,n_regionkey,n_name) <-- nation) Yield (n_regionkey,n_nationkey)

      val regionNameNations = For(
        (n_regionkey,n_nationkey,r_name) <-- regionNations.join(region)
      ) Yield (n_nationkey,r_name)

      val customerNations = For ((c_custkey,c_name,c_nationkey) <-- customer) Yield (c_nationkey,c_name)

      val query = Group(
          For (
            (__,custName0,regionName0) <-- customerNations.join(regionNameNations)
          ) Yield (regionName0,custName0)
      )
      query
    }

    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {

      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val inputs = List(rdds.nation, rdds.region, rdds.customer)
      val dstream = rddToDStream(rdds.customer, batchSize, ssc)
      val customer = Bag(dstream, "customer".narrow)
      val query = genQuery(rdds, customer)
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
    println(s"Query - Q4. Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }



  test("Q5") {

    def genQuery[V,ID](rdds: TpchRdds, partSupp: LiteralExpr[V,ID]) = {
      val part = Bag(rdds.part, "part".narrow)
      val suppliers = Var("X1")

      val partSuppliers = Group(partSupp)
      val query = For (
        (ps_partkey,suppliers,p_name) <-- partSuppliers.join(part)
      ) Yield (p_name,suppliers)
      query
    }

    def testGen(sampleName: String,
                batchSize: Int,
                shredded: Boolean,
                acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {

      println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
      val rdds = new TpchRdds(sampleName)(spark)
      val inputs = List(rdds.partSupp, rdds.part)
      val dstream = rddToDStream(rdds.partSupp, batchSize, ssc)
      val partSupp = Bag(dstream, "partSupp".narrow)
      val query = genQuery(rdds, partSupp)
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
    println(s"Query - Q5. Dataset - $sampleName")
    tests.foreach(singleDStreamTest)
    println()
  }

    test("Q6") {

      def genQuery[V,ID](rdds: TpchRdds, partSupp: LiteralExpr[V,ID]) = {
        val parts = Var("X")
        val supplier = Bag(rdds.supplier, "supplier.narrow")
        val supplierParts = Group(
          For ((ps_partkey,ps_suppkey) <-- partSupp) Yield (ps_suppkey,ps_partkey)
        )
        val nationSupplierParts = Group(
          For (
            (s_suppkey,s_name,s_nationkey,parts) <-- supplier.join(supplierParts)
          ) Yield (s_name,s_nationkey,parts)
        )

        nationSupplierParts
      }

      def testGen(sampleName: String,
                  batchSize: Int,
                  shredded: Boolean,
                  acc: Boolean): ((SparkSession,StreamingContext) => Unit) = (spark: SparkSession, ssc: StreamingContext) => {

        println(s"batchSize = $batchSize, shredded = $shredded, acc = $acc")
        val rdds = new TpchRdds(sampleName)(spark)
        val inputs = List(rdds.partSupp, rdds.supplier)
        val dstream = rddToDStream(rdds.partSupp, batchSize, ssc)
        val partSupp = Bag(dstream, "partSupp".narrow)
        val query = genQuery(rdds, partSupp)
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
      println(s"Query - Q6. Dataset - $sampleName")
      tests.foreach(singleDStreamTest)
      println()
    }
}
