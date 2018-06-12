import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import slender.{Collection, KeyExpr, NumericExpr, PhysicalCollection, RingExpr, VariableExpr}

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

  case class PairRDD[K,R](rdd: RDD[(K,R)])

  implicit def toPairRDD[K, R](rdd: RDD[(K, R)]): PairRDD[K, R] = PairRDD(rdd)

  implicit def fromPairRDD[K,R](pairRdd: PairRDD[K,R]): RDD[(K,R)] = pairRdd.rdd

  implicit def pairRDDtoPairRDDFunctions[K : ClassTag, R : ClassTag](pairRdd: PairRDD[K,R]): PairRDDFunctions[K,R] =
    new PairRDDFunctions(pairRdd.rdd)

  implicit def mapToPhysicalCollection[K,R](map: Map[K,R])
                                           (implicit coll: Collection[Map,K,R]): PhysicalCollection[Map,K,R] =
    PhysicalCollection(map)

  implicit def setToPhysicalBag[K](set: Set[K])
                                  (implicit coll: Collection[Map,K,Int]): PhysicalCollection[Map,K,Int] =
    PhysicalCollection(set.map(k => (k,1)).toMap)

  implicit def rddToPhysicalCollection[K,R](rdd: RDD[(K,R)])
                                           (implicit coll: Collection[PairRDD,K,R]): PhysicalCollection[PairRDD,K,R] =
    PhysicalCollection(rdd)

  implicit def rddToPhysicalBag[K](rdd: RDD[K])(implicit coll: Collection[PairRDD,K,Int]): PhysicalCollection[PairRDD,K,Int] =
    PhysicalCollection[PairRDD, K, Int](rdd.map(k => (k,1)))

  object dsl extends Syntax with Variables
}


