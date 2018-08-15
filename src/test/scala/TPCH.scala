package slender

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class TpchRdds(sampleName: String = "10_customers")(implicit spark: SparkSession) {

  import spark.implicits._

  private def loadTable(name: String): DataFrame =
    spark
      .read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"/Users/theo/coursework/project/data/tpch/$sampleName/$name.csv")
      .distinct

  lazy val customer = loadTable("customer")
    .select("c_custkey", "c_name", "c_nationkey")
    .as[(Int,String,Int)].rdd//.productsToHlists

  lazy val orders = loadTable("orders")
    .select("o_orderkey", "o_custkey", "o_orderdate")
    .as[(Int,Int,Timestamp)].rdd//.productsToHlists

  lazy val lineitem = loadTable("lineitem")
    .select("l_orderkey","l_partkey","l_suppkey")
    .as[(Int,Int,Int)].rdd//.productsToHlists

  lazy val part = loadTable("part")
    .select("p_partkey","p_name")
    .as[(Int,String)].rdd//.productsToHlists

  lazy val partSupp = loadTable("partsupp")
    .select("ps_partkey", "ps_suppkey")
    .as[(Int,Int)].rdd//.productsToHlists

  lazy val supplier = loadTable("supplier")
    .select("s_suppkey","s_name","s_nationkey")
    .as[(Int,String,Int)].rdd//.productsToHlists

  lazy val nation = loadTable("nation")
    .select("n_nationkey", "n_regionkey", "n_name")
    .as[(Int,Int,String)].rdd//.productsToHlists

  lazy val region = loadTable("region")
    .select("r_regionkey", "r_name")
    .as[(Int,String)].rdd//.productsToHlists

}

class TpchLocal(sampleName: String = "10_customers")(implicit spark: SparkSession) {

  private val rdds = new TpchRdds(sampleName)

  private def toLocalCollection[T](rdd: => RDD[T]): LiteralExpr[Map[T,Int],String] = LiteralExpr(rdd.collect.map(t => (t,1)).toMap,"")

  lazy val customer = toLocalCollection(rdds.customer)
  lazy val orders = toLocalCollection(rdds.orders)
  lazy val lineitem = toLocalCollection(rdds.lineitem)
  lazy val part = toLocalCollection(rdds.part)
  lazy val partSupp = toLocalCollection(rdds.partSupp)
  lazy val supplier = toLocalCollection(rdds.supplier)
  lazy val nation = toLocalCollection(rdds.nation)
  lazy val region = toLocalCollection(rdds.region)

}

class TpchDstreams(sampleName: String, n: Int, rep: Int = 1)(implicit spark: SparkSession, ssc: StreamingContext) {

  private val rdds = new TpchRdds(sampleName)

  lazy val customer = rddToDStream(rdds.customer, n)
  lazy val orders = rddToDStream(rdds.orders, n)
  lazy val lineitem = rddToDStream(rdds.lineitem, n)
  lazy val part = rddToDStream(rdds.part, n)
  lazy val partSupp = rddToDStream(rdds.partSupp, n)
  lazy val supplier = rddToDStream(rdds.supplier, n)
  lazy val nation = rddToDStream(rdds.nation, n)
  lazy val region = rddToDStream(rdds.region, n)
}
