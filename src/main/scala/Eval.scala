package slender

import shapeless.{::, HList, HNil}

trait Eval[-E,+T] extends ((E,BoundVars) => T) with Serializable

object Eval {

  def instance[E,T](f: (E,BoundVars) => T): Eval[E,T] = new Eval[E,T] {
    def apply(v1: E, v2: BoundVars): T = f(v1,v2)
  }

  implicit def VariableEval[T]: Eval[TypedVariable[T],T] = instance {
    case (tv,bvs) => bvs(tv.name).asInstanceOf[T]
  }

  implicit def UnusedVariableEval: Eval[UnusedVariable,Any] = instance { (_,_) => null }

  implicit def LiteralEval[V]: Eval[LiteralExpr[V],V] = instance { (v1, _) => v1.value }

  implicit def InfiniteMappingEval[V,R,KT,RT]
  (implicit evalK: Eval[V,KT], evalR: Eval[R,RT], varBinder: Binder[V, KT]): Eval[InfiniteMappingExpr[V,R],KT => RT] =
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

  implicit def SumEval[E,C[_,_],K,R,O]
  (implicit eval: Eval[E,C[K,R]], sum: Sum[C[K,R],O]): Eval[SumExpr[E],O] =
    instance { case (SumExpr(c),vars) => sum(eval(c,vars)) }

  implicit def GroupEval[E,C[_,_],K,R,O]
  (implicit eval: Eval[E,C[K,R]], group: Group[C[K,R],O]): Eval[GroupExpr[E],O] =
    instance { case (GroupExpr(c),vars) => group(eval(c,vars)) }

  implicit def NotEval[E,T](implicit eval: Eval[E,T], ring: Ring[T]): Eval[NotExpr[E],T] =
    instance { case (NotExpr(c),vars) => ring.not(eval(c,vars)) }

  implicit def NegateEval[E,T](implicit eval: Eval[E,T], ring: Ring[T]): Eval[NegateExpr[E],T] =
    instance { case (NegateExpr(c),vars) => ring.negate(eval(c,vars)) }

  implicit def SngEval[K,R,TK,TR]
  (implicit evalK: Eval[K,TK], evalR: Eval[R,TR]): Eval[SngExpr[K,R],Map[TK,TR]] =
    instance { case (SngExpr(k,r),vars) => Map(evalK(k,vars) -> evalR(r,vars)) }

  implicit def PredicateEval[K1,K2,T1,T2]
  (implicit eval1: Eval[K1,T1], eval2: Eval[K2,T2]): Eval[Predicate[K1, K2, T1, T2], Int] =
    instance { case (Predicate(k1,k2,f,_),bvs) => if (f(eval1(k1,bvs),eval2(k2,bvs))) 1 else 0 }

  implicit def HNilEval: Eval[HNil,HNil] = instance[HNil,HNil] { case (HNil,_) => HNil }

  implicit def HConsEval[H,T <: HList,HO,TO <: HList]
    (implicit evalH: Eval[H,HO], evalT: Eval[T,TO]): Eval[H :: T, HO :: TO] = instance[H::T,HO::TO] {
      case (h :: t,bvs) => evalH(h, bvs) :: evalT(t, bvs)
    }

}


