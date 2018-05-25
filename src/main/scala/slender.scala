import org.apache.spark.rdd.{PairRDDFunctions, RDD}

package object slender extends AlgebraImplicits with ShreddingImplicits with EvalImplicits with Serializable {

  type BoundVars = Map[Variable[_],Any]

  case class PairRDD[K,R](rdd: RDD[(K,R)])

  implicit def toPairRDD[K, R](rdd: RDD[(K, R)]): PairRDD[K, R] = PairRDD(rdd)

  implicit def fromPairRDD[K,R](pairRdd: PairRDD[K,R]): RDD[(K,R)] = pairRdd.rdd

  implicit def pairRDDtoPairRDDFunctions[K,R](pairRdd: PairRDD[K,R]): PairRDDFunctions[K,R] = pairRdd.rdd

}
