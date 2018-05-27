import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.ClassTag

trait types {
  type BoundVars = Map[String,Any]
  type Untyped
}

package object slender extends types with Serializable {

  //courtesy of http://missingfaktor.blogspot.co.uk/2013/12/optional-implicit-trick-in-scala.html
  case class Perhaps[T](value: Option[T])

  implicit def perhaps[T](implicit ev: T = null): Perhaps[T] = Perhaps(Option(ev))

  case class PairRDD[K,R](rdd: RDD[(K,R)])

  implicit def toPairRDD[K, R](rdd: RDD[(K, R)]): PairRDD[K, R] = PairRDD(rdd)

  implicit def fromPairRDD[K,R](pairRdd: PairRDD[K,R]): RDD[(K,R)] = pairRdd.rdd

  implicit def pairRDDtoPairRDDFunctions[K : ClassTag, R : ClassTag](pairRdd: PairRDD[K,R]): PairRDDFunctions[K,R] =
    new PairRDDFunctions(pairRdd.rdd)

  implicit def mapToPhysicalCollection[K,R](map: Map[K,R]): PhysicalCollection[Map,K,R] = PhysicalCollection(map)

  implicit def rddToPhysicalCollection[K,R](rdd: RDD[(K,R)]): PhysicalCollection[PairRDD,K,R] = PhysicalCollection(rdd)

  object implicits extends AlgebraImplicits with ShreddingImplicits with EvalImplicits
    with DSL with VariableExprImplicits with VariableResolutionImplicits
}


