package slender

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

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