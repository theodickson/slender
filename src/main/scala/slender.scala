import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.ClassTag

package object slender extends AlgebraImplicits with ShreddingImplicits with EvalImplicits with DSL with Serializable {

  type BoundVars = Map[Variable[_],Any]

  case class PairRDD[K,R](rdd: RDD[(K,R)])

  implicit def toPairRDD[K, R](rdd: RDD[(K, R)]): PairRDD[K, R] = PairRDD(rdd)

  implicit def fromPairRDD[K,R](pairRdd: PairRDD[K,R]): RDD[(K,R)] = pairRdd.rdd

  implicit def pairRDDtoPairRDDFunctions[K : ClassTag, R : ClassTag](pairRdd: PairRDD[K,R]): PairRDDFunctions[K,R] =
    new PairRDDFunctions(pairRdd.rdd)

  implicit def mapToPhysicalCollection[K,R](map: Map[K,R]): PhysicalCollection[Map,K,R] = PhysicalCollection(map)

  implicit def rddToPhysicalCollection[K,R](rdd: RDD[(K,R)]): PhysicalCollection[PairRDD,K,R] = PhysicalCollection(rdd)

}
