package slender

import org.apache.spark.streaming.{State, StateSpec}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

trait AggregateDStream[In <: SDStream[In], Out <: SDStream[Out]] extends (In => Out) with Serializable

object AggregateDStream {

  implicit def incrementalDStream[K:ClassTag,V:ClassTag]
  (implicit ring: Ring[V]): AggregateDStream[IncDStream.Aux[(K,V)],AggDStream.Aux[(K,V)]] =
    new AggregateDStream[IncDStream.Aux[(K,V)],AggDStream.Aux[(K,V)]] {
    def apply(v1: IncDStream.Aux[(K,V)]): AggDStream.Aux[(K,V)] = {
      val mappingFunction: (K,Option[V],State[V]) => Unit = (_,value,state) => {
        val previous = state.getOption.getOrElse(ring.zero)
        val latest = value.getOrElse(ring.zero)
        val current = ring.add(previous, latest)
        state.update(current)
      }
      val stateSpec = StateSpec.function[K,V,V,Unit](mappingFunction)
      AggDStream(v1.dstream.mapWithState(stateSpec).stateSnapshots)
    }

  }

  implicit def aggregateDStream[T]: AggregateDStream[AggDStream.Aux[T],AggDStream.Aux[T]] =
    new AggregateDStream[AggDStream.Aux[T],AggDStream.Aux[T]] {
    def apply(v1: AggDStream.Aux[T]): AggDStream.Aux[T] = v1
  }
}

//

//  //todo - will clash with the above when the key is also a valid ring
//  def apply[R:ClassTag](dstream: IncDStream.Aux[R])(implicit ring: Ring[R]): AggDStream.Aux[R] = {
//    val mappingFunction: (Unit,Option[R],State[R]) => Unit = (_,value,state) => {
//      val previous = state.getOption.getOrElse(ring.zero)
//      val latest = value.getOrElse(ring.zero)
//      val current = ring.add(previous, latest)
//      state.update(current)
//    }
//
//    val stateSpec = StateSpec.function[Unit,R,R,Unit](mappingFunction)
//    AggDStream(dstream.dstream.map(((),_)).mapWithState(stateSpec).stateSnapshots().map(_._2))
//  }

  //do nothing for dstreams which are already cumulative
//  def apply[T](dstream: AggDStream.Aux[T]): AggDStream.Aux[T] = dstream
//
//
//
//}
