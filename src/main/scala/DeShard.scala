package slender

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

trait DeShard[T,S] extends (T => S)

object DeShard {

 implicit def RddDeShard[K,R](implicit ring: Ring[R]): DeShard[RDD[(K,R)],RDD[(K,R)]] = new DeShard[RDD[(K,R)],RDD[(K,R)]] {
   def apply(v1: RDD[(K,R)]): RDD[(K,R)] = v1.reduceByKey(ring.add)
 }

  implicit def DStreamDeShard[K,R](implicit ring: Ring[R]): DeShard[DStream[(K,R)],DStream[(K,R)]] = new DeShard[DStream[(K,R)],RDD[(K,R)]] {
    def apply(v1: DStream[(K,R)]): RDD[(K,R)] = {
      var out: Option[RDD[(K,R)]] = None
      v1.foreachRDD { rdd => out.fold(rdd)(_.union(rdd)) }

    }
  }
}