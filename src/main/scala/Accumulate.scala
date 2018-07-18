package slender

import org.apache.spark.streaming.{State, StateSpec}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**Produce the cumulative version of an output.
  * This is obviously only applicable when the output is a DStream. In all other cases this will simply be the identity
  * function.
  * When the output is a DStream, it will also be the identity if it is already a cumulative DStream.
  * If the output is an incremental DStream, it will use a stateful mapping to efficiently aggregate the value for each key,
  * if it is a collection. If it is a distributed single ring value, it will just aggregate all values.
  */
trait Accumulate[-In, +Out] extends (In => Out) with Serializable

object Accumulate extends LowPriorityAccumulateImplicits {

  implicit def incrementalCollection[K:ClassTag,V:ClassTag]
  (implicit ring: Ring[V]): Accumulate[IncDStream.Aux[(K,V)],AggDStream.Aux[(K,V)]] =
    new Accumulate[IncDStream.Aux[(K,V)],AggDStream.Aux[(K,V)]] {
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

  implicit def incrementalValue[R:ClassTag]
  (implicit ring: Ring[R]): Accumulate[IncDStream.Aux[R],AggDStream.Aux[R]] =
    new Accumulate[IncDStream.Aux[R],AggDStream.Aux[R]] {
      def apply(v1: IncDStream.Aux[R]): AggDStream.Aux[R] = {
        val mappingFunction: (Unit,Option[R],State[R]) => Unit = (_,value,state) => {
          val previous = state.getOption.getOrElse(ring.zero)
          val latest = value.getOrElse(ring.zero)
          val current = ring.add(previous, latest)
          state.update(current)
        }
        val stateSpec = StateSpec.function[Unit,R,R,Unit](mappingFunction)
        AggDStream(v1.dstream.map(((),_)).mapWithState(stateSpec).stateSnapshots().map(_._2))
      }

    }

//  implicit def aggregate[T]: Accumulate[AggDStream.Aux[T],AggDStream.Aux[T]] =
//    new Accumulate[AggDStream.Aux[T],AggDStream.Aux[T]] {
//    def apply(v1: AggDStream.Aux[T]): AggDStream.Aux[T] = v1
//  }
}

trait LowPriorityAccumulateImplicits {
  implicit def identity[T]: Accumulate[T,T] = new Accumulate[T,T] { def apply(v1: T): T = v1 }
}

