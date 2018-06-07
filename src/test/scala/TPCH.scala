package slender

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
    .as[(Int,String,Int)].rdd

  lazy val orders = loadTable("orders")
    .select("o_orderkey", "o_custkey", "o_orderdate")
    .as[(Int,Int,Timestamp)].rdd

  lazy val lineitem = loadTable("lineitem")
    .select("l_orderkey","l_partkey","l_suppkey")
    .as[(Int,Int,Int)].rdd

  lazy val part = loadTable("part")
    .select("p_partkey","p_name")
    .as[(Int,String)].rdd

  lazy val partSupp = loadTable("partsupp")
    .select("ps_partkey", "ps_suppkey")
    .as[(Int,Int)].rdd

  lazy val supplier = loadTable("supplier")
    .select("s_suppkey","s_name","s_nationkey")
    .as[(Int,String,Int)].rdd

  lazy val nation = loadTable("nation")
    .select("n_nationkey", "n_name")
    .as[(Int,String)].rdd

  lazy val region = loadTable("region")
    .select("r_regionkey", "r_name")
    .as[(Int,String)].rdd

}

class TpchLocal(sampleName: String = "10_customers")(implicit spark: SparkSession) {

  private val rdds = new TpchRdds(sampleName)

  private def toLocalCollection[T](rdd: => RDD[T]): Map[T,Int] = rdd.collect.map(t => (t,1)).toMap

  lazy val customer = toLocalCollection(rdds.customer)
  lazy val orders = toLocalCollection(rdds.orders)
  lazy val lineitem = toLocalCollection(rdds.lineitem)
  lazy val part = toLocalCollection(rdds.part)
  lazy val partSupp = toLocalCollection(rdds.partSupp)
  lazy val supplier = toLocalCollection(rdds.supplier)
  lazy val nation = toLocalCollection(rdds.nation)
  lazy val region = toLocalCollection(rdds.region)

}
