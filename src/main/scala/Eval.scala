package slender

import org.apache.spark.rdd.RDD
import shapeless.{::, HList, HNil}

import scala.reflect.ClassTag

trait Eval[-E,+T] extends ((E,Namespace) => T) with Serializable

object Eval {

  def instance[E,T](f: (E,Namespace) => T): Eval[E,T] = new Eval[E,T] {
    def apply(v1: E, v2: Namespace): T = f(v1,v2)
  }

  implicit def VariableEval[T]: Eval[TypedVariable[T],T] = instance {
    case (tv,bvs) => bvs(tv.name).asInstanceOf[T]
  }

  implicit def CollectExprRddEval[E,K:ClassTag,R:ClassTag](implicit eval: Eval[E,RDD[(K,R)]]): Eval[CollectExpr[E],Map[K,R]] = instance {
    case (CollectExpr(e),vars) => eval(e,vars).collectAsMap().toMap
  }

//    instance {
//    case (CollectExpr(e),vars) => eval(e,vars).collectAsMap()
//  }

  implicit def UnusedVariableEval: Eval[UnusedVariable,Any] = instance { (_,_) => null }

  implicit def LiteralEval[V,ID]: Eval[LiteralExpr[V,ID],V] = instance { (v1, _) => v1.value }

  implicit def InfiniteMappingEval[V,R,KT,RT]
  (implicit evalK: Eval[V,KT], evalR: Eval[R,RT], varBinder: Binder[V,KT]): Eval[InfiniteMappingExpr[V,R],KT => RT] =
    instance { case (InfiniteMappingExpr(v,r),bvs) =>
      (k: KT) => evalR(r,bvs ++ varBinder(v, k))
    }

  implicit def MultiplyEval[E1,E2,T1,T2,O]
  (implicit evalL: Eval[E1,T1], evalR: Eval[E2,T2], mult: Multiply[T1,T2,O]): Eval[MultiplyExpr[E1,E2],O] =
    instance { case (MultiplyExpr(l,r),vars) => mult(evalL(l,vars),evalR(r,vars)) }

  implicit def DotEval[E1,E2,T1,T2,O]
  (implicit evalL: Eval[E1,T1], evalR: Eval[E2,T2], dot: Dot[T1,T2,O]): Eval[DotExpr[E1,E2],O] =
    instance { case (DotExpr(l,r),vars) => dot(evalL(l,vars),evalR(r,vars)) }

  implicit def JoinEval[E1,E2,T1,T2,O]
  (implicit evalL: Eval[E1,T1], evalR: Eval[E2,T2], join: Join[T1,T2,O]): Eval[JoinExpr[E1,E2],O] =
    instance { case (JoinExpr(l,r),vars) => join(evalL(l,vars),evalR(r,vars)) }

  implicit def AddEval[E1,E2,T]
  (implicit evalL: Eval[E1,T], evalR: Eval[E2,T], ring: Ring[T]): Eval[AddExpr[E1,E2],T] =
    instance { case (AddExpr(l,r),vars) => ring.add(evalL(l,vars),evalR(r,vars)) }

  implicit def SumEval[E,C,O]
  (implicit eval: Eval[E,C], sum: Sum[C,O]): Eval[SumExpr[E],O] =
    instance { case (SumExpr(c),vars) => sum(eval(c,vars)) }

  implicit def GroupEval[E,C,O]
  (implicit eval: Eval[E,C], group: Group[C,O]): Eval[GroupExpr[E],O] =
    instance { case (GroupExpr(c),vars) => group(eval(c,vars)) }

  implicit def NotEval[E,T](implicit eval: Eval[E,T], ring: Ring[T]): Eval[NotExpr[E],T] =
    instance { case (NotExpr(c),vars) => ring.not(eval(c,vars)) }

  implicit def NegateEval[E,T](implicit eval: Eval[E,T], ring: Ring[T]): Eval[NegateExpr[E],T] =
    instance { case (NegateExpr(c),vars) => ring.negate(eval(c,vars)) }

  implicit def SngEval[K,R,TK,TR]
  (implicit evalK: Eval[K,TK], evalR: Eval[R,TR]): Eval[SngExpr[K,R],Map[TK,TR]] =
    instance { case (SngExpr(k,r),vars) => Map(evalK(k,vars) -> evalR(r,vars)) }

  implicit def ApplyExprEval[K,T,U]
  (implicit eval: Eval[K,T]): Eval[ApplyExpr[K,T,U],U] = instance {
    case (ApplyExpr(k,f),bvs) => f(eval(k,bvs))
  }

  implicit def HNilEval: Eval[HNil,HNil] = instance[HNil,HNil] { case (HNil,_) => HNil }

  implicit def HConsEval[H,T <: HList,HO,TO <: HList]
    (implicit evalH: Eval[H,HO], evalT: Eval[T,TO]): Eval[H :: T, HO :: TO] = instance[H::T,HO::TO] {
      case (h :: t,bvs) => evalH(h, bvs) :: evalT(t, bvs)
    }
}


