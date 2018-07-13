package slender

import org.apache.spark.rdd.RDD
import shapeless.ops.hlist.Prepend
import shapeless.{::, HList, HNil}

import scala.reflect.ClassTag

case class Label[Expr,K1](value: K1) extends Serializable

case class ShreddedResult[+Flat,+Ctx <: HList](flat: Flat, ctx: Ctx) {
  def nested[N,Tupled](implicit lookup: Lookup[Flat,Ctx,N], tupler: DeepTupler[N,Tupled]) = tupler(lookup(flat,ctx))
}

//todo - rename/redesign?
trait GroupLabeller[-In,+Out,+Dict,Expr] extends (In => (Out,Dict)) with Serializable

object GroupLabeller {
  //see discussion on Group for why these are not done generically
  implicit def rdd2[K1:ClassTag,K2:ClassTag,R,Expr]
  (implicit group: Group[RDD[(K1::K2::HNil,R)],RDD[(K1::Map[K2,R]::HNil,Boolean)]]):
    GroupLabeller[RDD[(K1::K2::HNil,R)],RDD[(K1::Label[Expr,K1]::HNil,Boolean)],RDD[(Label[Expr,K1],Map[K2,R])],Expr] =
    new GroupLabeller[RDD[(K1::K2::HNil,R)],RDD[(K1::Label[Expr,K1]::HNil,Boolean)],RDD[(Label[Expr,K1],Map[K2,R])],Expr] {
      def apply(v1: RDD[(K1::K2::HNil,R)]): (RDD[(K1::Label[Expr,K1]::HNil,Boolean)],RDD[(Label[Expr,K1],Map[K2,R])]) = {
        //val labelled = v1.map { case ((k1::k2),r) => ((Label(k1)::k2,r)) }
        val grouped = group(v1)
        val flat = grouped.map { case (k1::_,i) => (k1::Label[Expr,K1](k1)::HNil,i) }
        val dict = grouped.map { case (k1::k2s::HNil,_) => (Label[Expr,K1](k1),k2s) }
        (flat,dict)
      }
    }

  implicit def rdd3[K1:ClassTag,K2:ClassTag,K3:ClassTag,R,Expr]
  (implicit group: Group[RDD[(K1::K2::K3::HNil,R)],RDD[(K1::Map[K2::K3::HNil,R]::HNil,Boolean)]]):
  GroupLabeller[RDD[(K1::K2::K3::HNil,R)],RDD[(K1::Label[Expr,K1]::HNil,Boolean)],RDD[(Label[Expr,K1],Map[K2::K3::HNil,R])],Expr] =
    new GroupLabeller[RDD[(K1::K2::K3::HNil,R)],RDD[(K1::Label[Expr,K1]::HNil,Boolean)],RDD[(Label[Expr,K1],Map[K2::K3::HNil,R])],Expr] {
      def apply(v1: RDD[(K1::K2::K3::HNil,R)]): (RDD[(K1::Label[Expr,K1]::HNil,Boolean)],RDD[(Label[Expr,K1],Map[K2::K3::HNil,R])]) = {
        //val labelled = v1.map { case ((k1::k2),r) => ((Label(k1)::k2,r)) }
        val grouped = group(v1)
        val flat = grouped.map { case (k1::_,i) => (k1::Label[Expr,K1](k1)::HNil,i) }
        val dict = grouped.map { case (k1::k2s::HNil,_) => (Label[Expr,K1](k1),k2s) }
        (flat,dict)
      }
    }
}


trait ShreddedEval[-Expr,+Flat,+Ctx <: HList] extends ((Expr,BoundVars) => ShreddedResult[Flat,Ctx]) with Serializable

object ShreddedEval {

  def instance[E,T,C <: HList](f: (E,BoundVars) => ShreddedResult[T,C]): ShreddedEval[E,T,C] = new ShreddedEval[E,T,C] {
    def apply(v1: E, v2: BoundVars): ShreddedResult[T,C] = f(v1,v2)
  }

  implicit def literalShreddedEval[T,ID]: ShreddedEval[LiteralExpr[T,ID],T,HNil] = instance {
    case (t,_) => ShreddedResult(t.value,HNil)
  }

  implicit def MultiplyShreddedEval[E1,E2,F1,F2,F,C1 <: HList,C2 <: HList,C <: HList]
  (implicit eval1: ShreddedEval[E1,F1,C1], eval2: ShreddedEval[E2,F2,C2], mult: Multiply[F1,F2,F], prepend: Prepend.Aux[C1,C2,C]):
  ShreddedEval[MultiplyExpr[E1,E2],F,C] = instance {
    case (MultiplyExpr(e1,e2),bvs) => {
      val ShreddedResult(f1,c1) = eval1(e1,bvs)
      val ShreddedResult(f2,c2) = eval2(e2,bvs)
      ShreddedResult(mult(f1,f2),prepend(c1,c2))
    }
  }

  implicit def DotShreddedEval[E1,E2,F1,F2,F,C1 <: HList,C2 <: HList,C <: HList]
  (implicit eval1: ShreddedEval[E1,F1,C1], eval2: ShreddedEval[E2,F2,C2], dot: Dot[F1,F2,F], prepend: Prepend.Aux[C1,C2,C]):
  ShreddedEval[DotExpr[E1,E2],F,C] = instance {
    case (DotExpr(e1,e2),bvs) => {
      val ShreddedResult(f1,c1) = eval1(e1,bvs)
      val ShreddedResult(f2,c2) = eval2(e2,bvs)
      ShreddedResult(dot(f1,f2),prepend(c1,c2))
    }
  }

  implicit def JoinShreddedEval[E1,E2,F1,F2,F,C1 <: HList,C2 <: HList,C <: HList]
  (implicit eval1: ShreddedEval[E1,F1,C1], eval2: ShreddedEval[E2,F2,C2], join: Join[F1,F2,F], prepend: Prepend.Aux[C1,C2,C]):
  ShreddedEval[JoinExpr[E1,E2],F,C] = instance {
    case (JoinExpr(e1,e2),bvs) => {
      val ShreddedResult(f1,c1) = eval1(e1,bvs)
      val ShreddedResult(f2,c2) = eval2(e2,bvs)
      ShreddedResult(join(f1,f2),prepend(c1,c2))
    }
  }

  implicit def SumShreddedEval[E1,F1,F,C <: HList]
  (implicit eval: ShreddedEval[E1,F1,C], sum: Sum[F1,F]): ShreddedEval[SumExpr[E1],F,C] = instance {
    case (SumExpr(e),bvs) => {
      val ShreddedResult(f1,c) = eval(e,bvs)
      ShreddedResult(sum(f1),c)
    }
  }

  implicit def GroupShreddedEval[InExpr,InFlat,InCtx <: HList,Flat,Dict]
  (implicit eval: ShreddedEval[InExpr,InFlat,InCtx], label: GroupLabeller[InFlat,Flat,Dict,InExpr]):
  ShreddedEval[GroupExpr[InExpr],Flat,Dict::InCtx] = instance {
    case (GroupExpr(e),bvs) => {
      val ShreddedResult(inFlat,inCtx) = eval(e,bvs)
      val (flat,dict) = label(inFlat)
      ShreddedResult(flat,dict::inCtx)
    }
  }

  //note - this assume the RHS introduces no shredding context, i.e. its just used for something simple like
  //reshaping and does not invovle other collections, inner fromKs or grouping or whatever.
  //I think this works for now esp with how we've had to use a Group
  implicit def InfiniteMappingShreddedEval[K,R,KT,RT]
  (implicit evalK: Eval[K,KT], evalR: Eval[R,RT], varBinder: Binder[K,KT]): ShreddedEval[InfiniteMappingExpr[K,R],KT => RT,HNil] =
    instance { case (InfiniteMappingExpr(v,r),bvs) =>
      ShreddedResult((k: KT) => evalR(r,bvs ++ varBinder(v, k)), HNil)
    }

  //todo - sng not needed if all sngs are inside inf mappings

  //todo - all vars by defn inside inf mappings so they are not needed either

  //todo - note this is all very simplified as For/Yield only reshapes now

  //todo - predicate




}