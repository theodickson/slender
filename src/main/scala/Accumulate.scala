package slender

import org.apache.spark.streaming.{State, StateSpec}

import scala.reflect.ClassTag

/** Accumulate is an interface which produces the cumulative version of a DStream.
  * For IncDStream inputs, it actually produces the cumulative version using mapWithState.
  * For AccDStream inputs, since they are already accumulated, it just passes them through unchanged.
  */
trait Accumulate[In <: SDStream[In], T] extends (SDStream.Aux[In,T] => AggDStream.Aux[T]) with Serializable

object Accumulate {

  /**Accumulate a DStream representing a collection*/
  implicit def incrementalCollection[K:ClassTag,V:ClassTag]
  (implicit ring: Ring[V]): Accumulate[IncDStream,(K,V)] =
    new Accumulate[IncDStream, (K,V)] {
    def apply(v1: IncDStream.Aux[(K,V)]): AggDStream.Aux[(K,V)] = {
      //The function applied to each key-value pair in the state store with each batch:
      val mappingFunction: (K,Option[V],State[V]) => Unit = (_,value,state) => {
        //get the previous value for the key, intialising it as zero if not present
        val previous = state.getOption.getOrElse(ring.zero)
        //get the value for the key in the batch being processed:
        val latest = value.getOrElse(ring.zero)
        //add them together and update the state:
        val current = ring.add(previous, latest)
        state.update(current)
      }
      val stateSpec = StateSpec.function[K,V,V,Unit](mappingFunction)
      //Return an AggDstream wrapping the stream of state snapshots, which are they cumulative key-value pairs.
      AggDStream(v1.dstream.mapWithState(stateSpec).stateSnapshots)
    }

  }

  /**Accumulate a DStream representing a single distributed ring value (e.g. from applying sum)*/
  implicit def incrementalValue[R:ClassTag]
  (implicit ring: Ring[R]): Accumulate[IncDStream,R] =
    new Accumulate[IncDStream,R] {
      def apply(v1: IncDStream.Aux[R]): AggDStream.Aux[R] = {
        //As above but there are no keys so we use Unit to represent this, we are just maintaining a single value.
        val mappingFunction: (Unit,Option[R],State[R]) => Unit = (_,value,state) => {
          val previous = state.getOption.getOrElse(ring.zero)
          val latest = value.getOrElse(ring.zero)
          val current = ring.add(previous, latest)
          state.update(current)
        }
        val stateSpec = StateSpec.function[Unit,R,R,Unit](mappingFunction)
        //When producing the output stream, we also map to the second element to ignore the Unit key.
        AggDStream(v1.dstream.map(((),_)).mapWithState(stateSpec).stateSnapshots().map(_._2))
      }

    }

  /**For already cumulative DStreams, just do nothing*/
  implicit def identity[T]: Accumulate[AggDStream,T] = new Accumulate[AggDStream,T] {
    def apply(v1: AggDStream.Aux[T]): AggDStream.Aux[T] = v1
  }

}