package slender

import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

trait SDStream[Self <: SDStream[Self]] { self: Self =>
  type T
//  implicit def tag: ClassTag[T]
  def dstream: DStream[T]
  def map[S](f: DStream[T] => DStream[S]): SDStream.Aux[Self,S]
  def acc(implicit accumulate: Accumulate[Self,T]): SDStream.Aux[AggDStream,T] = accumulate(self)
  def print: Unit = dstream.print
  def print(num: Int): Unit = dstream.print(num)
}

object SDStream {
  type Aux[Sub <: SDStream[Sub],T0] = SDStream[Sub] { type T = T0 }
}

trait IncDStream extends SDStream[IncDStream]

object IncDStream {

  type Aux[T0] = SDStream.Aux[IncDStream,T0]

  def apply[T0](dstream0: DStream[T0]): SDStream.Aux[IncDStream, T0] = new IncDStream {
    type T = T0
    val dstream = dstream0
    def map[S](f: DStream[T] => DStream[S]): SDStream.Aux[IncDStream, S] = IncDStream(f(dstream))
  }
}

trait AggDStream extends SDStream[AggDStream]

object AggDStream {

  type Aux[T0] = SDStream.Aux[AggDStream,T0]

  def apply[T0](dstream0: DStream[T0]): SDStream.Aux[AggDStream,T0] = new AggDStream {
    type T = T0
    val dstream = dstream0
    def map[S](f: DStream[T] => DStream[S]): SDStream.Aux[AggDStream,S] = AggDStream(f(dstream))
  }
}