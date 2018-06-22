import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.{Encoder, SparkSession}

import scala.reflect.ClassTag

trait types {
  type BoundVars = Map[String,Any]
  type Untyped
}

package object slender extends types with Serializable {

  implicit def boolToInt(b: Boolean): Int = if (b) 1 else 0

  //courtesy of http://missingfaktor.blogspot.co.uk/2013/12/optional-implicit-trick-in-scala.html
  case class Perhaps[T](value: Option[T])

  implicit def perhaps[T](implicit ev: T = null): Perhaps[T] = Perhaps(Option(ev))

  case class PairRDD[K,R](rdd: RDD[(K,R)]) {
    def show(implicit ss: SparkSession, enc: Encoder[(K,R)]): Unit = {
      import ss.implicits._
      rdd.toDF.show
    }
  }

  implicit def toPairRDD[K, R](rdd: RDD[(K, R)]): PairRDD[K, R] = PairRDD(rdd)

  implicit def fromPairRDD[K,R](pairRdd: PairRDD[K,R]): RDD[(K,R)] = pairRdd.rdd

  implicit def pairRDDtoPairRDDFunctions[K : ClassTag, R : ClassTag](pairRdd: PairRDD[K,R]): PairRDDFunctions[K,R] =
    new PairRDDFunctions(pairRdd.rdd)
//
//  def mapToPhysicalCollection[K,R](map: Map[K,R])
//                                           (implicit coll: Collection[Map,K,R]): PhysicalCollection[Map,K,R] =
//    PhysicalCollection(map)
//
//  def setToPhysicalBag[K](set: Set[K])
//                                  (implicit coll: Collection[Map,K,Int]): PhysicalCollection[Map,K,Int] =
//    PhysicalCollection(set.map(k => (k,1)).toMap)
//
//  def rddToPhysicalCollection[K,R](rdd: RDD[(K,R)])
//                                           (implicit coll: Collection[PairRDD,K,R]): PhysicalCollection[PairRDD,K,R] =
//    PhysicalCollection(rdd)
//
//  def rddToPhysicalBag[K](rdd: RDD[K])(implicit coll: Collection[PairRDD,K,Int]): PhysicalCollection[PairRDD,K,Int] =
//    PhysicalCollection[PairRDD, K, Int](rdd.map(k => (k,1)))

  def rddToPhysicalBag2[K](rdd: RDD[K]): LiteralExpr[PairRDD[K,Int]] =
    LiteralExpr[PairRDD[K,Int]](rdd.map(k => (k,1)))

  object dsl extends Syntax with Variables
}


