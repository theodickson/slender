package slender

import org.apache.spark.rdd.RDD
import shapeless._
import shapeless.ops.hlist.Tupler

trait DeepGeneric[T] extends Serializable {
  type Repr
  def to(t: T): Repr
  def from(r: Repr): T
}

//trait HDeepGeneric[T,S] extends Serializable {
//  def to(t: T): S
//  def from(s: S): T
//}
//
//object HDeepGeneric {
//
//  implicit def hconsDataGeneric[H,T <: HList,HR,TR <: HList](implicit genH: HDeepGeneric[H,HR], genT: HDeepGeneric[T,TR]):
//  HDeepGeneric[H :: T, HR :: TR] =
//    new HDeepGeneric[H :: T, HR :: TR] {
//      def to(v1: H :: T): HR :: TR = v1 match {
//        case (h :: t) => genH.to(h) :: genT.to(t)
//      }
//
//      def from(v1: HR :: TR): H :: T = v1 match {
//        case (hr :: tr) => genH.from(hr) :: genT.from(tr)
//      }
//    }
//
//  implicit def hnilDeepGeneric: HDeepGeneric[HNil,HNil] = new HDeepGeneric[HNil,HNil] {
//    def to(v1: HNil): HNil = HNil
//    def from(v1: HNil): HNil = HNil
//  }
//
//}

object DeepGeneric extends Priority1DeepGenericImplicits {

  type Aux[T,Repr0] = DeepGeneric[T] { type Repr = Repr0 }
  //todo - what happens with mixed data?

  def instance[T,Repr0](_to: T => Repr0,_from: Repr0 => T): DeepGeneric.Aux[T,Repr0] = new DeepGeneric[T] {
    type Repr = Repr0
    def to(t: T): Repr = _to(t)
    def from(s: Repr) : T = _from(s)
  }

  implicit def productDataGeneric[T <: Product,Repr0 <: HList,Repr <: HList]
  (implicit gen: Generic.Aux[T,Repr0], deepGen: DeepGeneric.Aux[Repr0,Repr]):
  DeepGeneric.Aux[T,Repr] = instance(
    t => deepGen.to(gen.to(t)),
    s => gen.from(deepGen.from(s))
  )

  implicit def mapDeepGeneric[K,V,K1,V1]
  (implicit deepK: DeepGeneric.Aux[K,K1], deepV: DeepGeneric.Aux[V,V1]): DeepGeneric.Aux[Map[K,V],Map[K1,V1]] =
    instance(
      _.map { case (k,v) => (deepK.to(k),deepV.to(v)) },
      _.map { case (k1,v1) => (deepK.from(k1),deepV.from(v1)) }
    )
}

trait Priority1DeepGenericImplicits extends Priority0DeepGenericImplicits {
  implicit def hconsDataGeneric[H,T <: HList,HR,TR <: HList](implicit genH: DeepGeneric.Aux[H,HR], genT: DeepGeneric.Aux[T,TR]):
  DeepGeneric.Aux[H :: T, HR::TR] =
    new DeepGeneric[H :: T] {
      type Repr = HR :: TR
      def to(v1: H :: T): Repr = v1 match {
        case (h :: t) => genH.to(h) :: genT.to(t)
      }

      def from(v1: Repr): H :: T = v1 match {
        case (hr :: tr) => genH.from(hr) :: genT.from(tr)
      }
    }

  implicit def hnilDeepGeneric: DeepGeneric.Aux[HNil,HNil] = new DeepGeneric[HNil] {
    type Repr = HNil
    def to(v1: HNil): Repr = HNil
    def from(v1: Repr): HNil = HNil
  }
}

trait Priority0DeepGenericImplicits {
  implicit def idDataGeneric[T]: DeepGeneric.Aux[T,T] = DeepGeneric.instance(identity[T],identity[T])
}


trait DeepTupler[T,Tupled] extends (T => Tupled) with Serializable
trait HDeepTupler[T,Tupled] extends (T => Tupled) with Serializable

object DeepTupler extends LowPriorityDeepTuplerImplicits {

  implicit def pairRddDeepTupler[K,V,KT,VT]
  (implicit kTupler: DeepTupler[K,KT], vTupler: DeepTupler[V,VT]): DeepTupler[RDD[(K,V)],RDD[(KT,VT)]] =
    new DeepTupler[RDD[(K,V)],RDD[(KT,VT)]] {
      def apply(v1: RDD[(K,V)]): RDD[(KT,VT)] = v1.map { case (k,v) => (kTupler(k),vTupler(v)) }
    }

  implicit def mapDeepTupler[K,V,KT,VT]
  (implicit kTupler: DeepTupler[K,KT], vTupler: DeepTupler[V,VT]): DeepTupler[Map[K,V],Map[KT,VT]] =
    new DeepTupler[Map[K,V],Map[KT,VT]] {
      def apply(v1: Map[K,V]): Map[KT,VT] = v1.map { case (k,v) => (kTupler(k),vTupler(v)) }
    }


  implicit def hlistDeepTupler[H,HT,T <: HList,TT <: HList,O]
  (implicit hTupler: DeepTupler[H,HT], tTupler: HDeepTupler[T,TT], tupler: Tupler.Aux[HT::TT,O]): DeepTupler[H::T,O] =
    new DeepTupler[H::T,O] {
      def apply(v1: H::T): O = tupler(hTupler(v1.head) :: tTupler(v1.tail))
    }
}

trait LowPriorityDeepTuplerImplicits {
  implicit def idDeepTupler[T]: DeepTupler[T,T] = new DeepTupler[T,T] {
    def apply(v1: T): T = v1
  }
}

object HDeepTupler {
  implicit def hconsDeepTupler[H,T <: HList,HT,TT <: HList]
  (implicit hTupler: DeepTupler[H,HT], tTupler: HDeepTupler[T,TT]): HDeepTupler[H::T,HT::TT] =
    new HDeepTupler[H::T,HT::TT] {
      def apply(v1: H::T): HT::TT = hTupler(v1.head) :: tTupler(v1.tail)
    }

  implicit def hnilDeepTupler: HDeepTupler[HNil,HNil] = new HDeepTupler[HNil,HNil] {
    def apply(v1: HNil): HNil = HNil
  }

}