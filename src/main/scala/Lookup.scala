package slender

import org.apache.spark.rdd.RDD
import shapeless._
import shapeless.ops.hlist.{Filter, Replacer}

import scala.reflect.ClassTag

//extract the label of type Label from within a key of type Key
trait Extract[-Key,+Label] extends (Key => Label) with Serializable

object Extract {
  //does not allow multiply matches
  implicit def extract[Key <: HList,Label](implicit filter: Filter.Aux[Key,Label,Label::HNil]): Extract[Key,Label] = new Extract[Key,Label] {
    def apply(v1: Key): Label = filter(v1).head
  }
}

//replace the label of type Label with the boxed ring of type Boxed within a key of Key, yielding a new key of type Replaced
trait Replace[-Key,Label,-Boxed,+Replaced] extends ((Key,Boxed) => Replaced) with Serializable

object Replace {
  implicit def replace[Key <: HList,Label,Boxed,Replaced]
  (implicit replacer: Replacer.Aux[Key,Label,Boxed,(Label,Replaced)]): Replace[Key,Label,Boxed,Replaced] = new Replace[Key,Label,Boxed,Replaced] {
    def apply(v1: Key, v2: Boxed): Replaced = replacer(v1,v2)._2
  }
}

trait Lookup[-Flat,-Ctx,+Nested] extends ((Flat,Ctx) => Nested) with Serializable

object Lookup {

  implicit def emptyContext[Flat]: Lookup[Flat,HNil,Flat] = new Lookup[Flat,HNil,Flat] {
    def apply(v1: Flat, v2: HNil): Flat = v1
  }

  implicit def inductive[Flat,H,T<:HList,Nested1,Nested2,Nested3]
  (implicit lookupH: Lookup[Flat,H,Nested1], lookupT: Lookup[Nested1,T,Nested2]): Lookup[Flat,H::T,Nested2] =
    new Lookup[Flat,H::T,Nested2] {
      def apply(v1: Flat, v2: H::T): Nested2 = lookupT(lookupH(v1,v2.head),v2.tail)
    }

  implicit def rdd[K,V,L:ClassTag,B,KR](implicit extract: Extract[K,L], replace: Replace[K,L,B,KR]): Lookup[RDD[(K,V)],RDD[(L,B)],RDD[(KR,V)]] =
    new Lookup[RDD[(K,V)],RDD[(L,B)],RDD[(KR,V)]] {
    def apply(v1: RDD[(K,V)], v2: RDD[(L,B)]): RDD[(KR,V)] = {
      //extract the label from the key, and make it the key of our RDD
      val extracted = v1.map { case (k,v) => (extract(k),(k,v)) }
      //join with the dictionary (relies on one entry per label), to 'add' the boxed ring to the row
      val joined = extracted.join(v2)
      //reshape, replacing the label within the key with our boxed ring value
      val out = joined.map { case (_,((k,v),boxed)) => (replace(k,boxed),v) }
      out
    }
  }
}

