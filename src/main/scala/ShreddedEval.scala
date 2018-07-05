package slender

import org.apache.spark.rdd.RDD
import shapeless.{::, HList, HNil, labelled}

import scala.reflect.ClassTag

case class Label[T](value: T) extends Serializable

//trait ShreddedResultBase[N] {
//  def nested: () => N
//}

case class ShreddedResult[Flat,Ctx](flat: Flat, ctx: Ctx)
//trait Lookup[T] extends
//trait Dictionary {
//  def lookup[T](t: T)(implicit lookup: Lookup[T])
//}
//sealed trait ShreddingContext[Flat] {
//  type Nested
//  def lookup(flat: Flat): Nested
//}
//
//case class UnitContext[T]() {
//  type Nested = T
//  def lookup(flat: T): T = flat
//}
//trait Nest[-Flat,-Ctx,+Nested] extends ((Flat,Ctx) => Nested) with Serializable
//
//object Nest {
//
//  def instance[Flat,Ctx,Nested](f: (Flat,Ctx) => Nested): Nest[Flat,Ctx,Nested] = new Nest[Flat,Ctx,Nested] {
//    def apply(v1: Flat, v2: Ctx): Nested = f(v1,v2)
//  }
//
//  implicit def LiteralNest[T,Ctx]: Nest[LiteralExpr[T],Ctx,T] = instance {
//
//  }
//}

//trait GroupLabeller[-In,+Out] extends (In => Out) with Serializable
//
//object GroupLabeller {
//
//  implicit def rdd[K1:ClassTag,K2 <: HList,R]: GroupLabeller[RDD[(K1::K2,R)],RDD[(K1,Label[K1])]] =
//    new GroupLabeller[RDD[(K1::K2,R)],RDD[(K1,Label[K1])]] {
//      def apply(v1: RDD[(K1::K2,R)]): RDD[(K1,Label[K1])] = v1.map(_._1.head).distinct.map(k1 => (k1,Label(k1)))
//    }
//}

//trait ShreddedResult[Flat,Ctx] {
//  type Nested
//  def nested: Nested
//}

trait GroupLabeller[-In,+Out,+Dict] extends (In => (Out,Dict)) with Serializable

object GroupLabeller {

  implicit def rdd[K1:ClassTag,K2 <: HList,R]
  (implicit group: Group[RDD[(K1::K2,R)],RDD[(K1::Map[K2,R]::HNil,Int)]]):
    GroupLabeller[RDD[(K1::K2,R)],RDD[(K1::Label[K1]::HNil,Int)],RDD[(Label[K1],Map[K2,R])]] =
    new GroupLabeller[RDD[(K1::K2,R)],RDD[(K1::Label[K1]::HNil,Int)],RDD[(Label[K1],Map[K2,R])]] {
      def apply(v1: RDD[(K1::K2,R)]): (RDD[(K1::Label[K1]::HNil,Int)],RDD[(Label[K1],Map[K2,R])]) = {
        //val labelled = v1.map { case ((k1::k2),r) => ((Label(k1)::k2,r)) }
        val grouped = group(v1)
        val flat = grouped.map { case (k1::_,i) => (k1::Label(k1)::HNil,i) }
        val dict = grouped.map { case (k1::k2s::HNil,_) => (Label(k1),k2s) }
        (flat,dict)
      }
    }
}


//trait ShreddingContext[C]

//trait ShreddedEval[-Expr,+Out,+Ctx] extends ((Expr,BoundVars) => (Out,Ctx)) with Serializable
//
//object ShreddedEval {
//
//  def instance[E,T,C](f: (E,BoundVars) => (T,C)): ShreddedEval[E,T,C] = new ShreddedEval[E,T,C] {
//    def apply(v1: E, v2: BoundVars): (T,C) = f(v1,v2)
//  }
//
//  implicit def literalShreddedEval[T]: ShreddedEval[LiteralExpr[T],T,Unit] = instance {
//    case (LiteralExpr(t),_) => (t,())
//  }
//
//  implicit def groupShreddedEval[E,O,C,Labelled,Flat,Dict]
//  (implicit eval: ShreddedEval[E,O,C], label: GroupLabeller[O,Flat,Dict]):
//    ShreddedEval[GroupExpr[E],Flat,?] = instance {
//    case (GroupExpr(e),bvs) => {
//      val (eF,eC) = eval(e,bvs)
//      val (flat,dict) = label(eF)
//
//      //eF :: (K1f,K2f) --> Rf (potentially contains some labels)
//      //eC :: a structure of dictionaries, including a method to turn eF into a (K1,K2) --> R by looking up the labels.
//      val nested = () => flat
//      (labelled,(grouped,eC))
//    }
//  }
//
//  implicit def sumShreddedEval[E,O,C,O1](eval: ShreddedEval[E,O,C], sum: Sum[O,O1]): ShreddedEval[SumExpr[E],O1,C] =
//    instance { case (SumExpr(e),bvs) => {
//        val (eF,eC) = eval(e,bvs)
//        (sum(eF),eC)
//      }
//    }
//}

trait ShreddedEval[Expr,Flat,Ctx] extends ((Expr,BoundVars) => ShreddedResult[Flat,Ctx]) with Serializable

object ShreddedEval {

  def instance[E,T,C](f: (E,BoundVars) => ShreddedResult[T,C]): ShreddedEval[E,T,C] = new ShreddedEval[E,T,C] {
    def apply(v1: E, v2: BoundVars): ShreddedResult[T,C] = f(v1,v2)
  }

  implicit def literalShreddedEval[T]: ShreddedEval[LiteralExpr[T],T,Unit] = instance {
    case (LiteralExpr(t),_) => ShreddedResult(t,())
  }

  implicit def groupShreddedEval[InExpr,InFlat,InCtx,InNested,Flat,Ctx]
  (implicit eval: ShreddedEval[InExpr,InFlat,InCtx,InNested], label: GroupLabeller[InFlat,Flat,Ctx]):
  ShreddedEval[GroupExpr[InExpr],Flat,?] = instance {
    case (GroupExpr(e),bvs) => {
      val ShreddedResult(inFlat,inCtx) = eval(e,bvs)
      val (flat,ctx) = label(inFlat)
      //eF :: (K1f,K2f) --> Rf (potentially contains some labels)
      //eC :: a structure of dictionaries, including a method to turn eF into a (K1,K2) --> R by looking up the labels.
      val nested = () => flat
      (labelled,(grouped,eC))
    }
  }

  implicit def sumShreddedEval[E,O,C,O1](eval: ShreddedEval[E,O,C], sum: Sum[O,O1]): ShreddedEval[SumExpr[E],O1,C] =
    instance { case (SumExpr(e),bvs) => {
      val ShreddedResult(eF,eC) = eval(e,bvs)
      ShreddedResult(sum(eF),eC)
    }
    }
}

object ShreddedResult {
  implicit def unitContext
}