package slender

import shapeless._

/** Resolve an expression containing untyped variables to one in which all variables are typed,
  * so that it can be evaluated.
  *
  * Untyped variables may be introduced in an infinite mapping. When this infinite mapping is multiplied by
  * a finite collection, we can infer the types of these untyped variables using
  * the key type of the finite collection. E.g. if we have the expression X * ((x,y) => 1) and X is a Bag[(Int,String)],
  * we can infer that the variable x must have type Int and y must have type String.
  *
  * Thus, we can resolve such any expression by:
  *   A. If it's a multiplication between a finite collection and an infinite mapping:
  *     1. Recursively resolve the finite collection.
  *     2. Traversing the infinite mapping (key and value) and appropriately
  *        tagging each untyped variable. (i.e. tag with the matching type if it's an untyped variable introduced in
  *        the key, otherwise ignore).
  *     3. Recursively resolving the value of the infinite mapping.
  *
  *   B. If it's any other expression, simply recursively resolve its children and rebuild.
  */
trait Resolver[-In,+Out] extends (In => Out) with Serializable


object Resolver extends Priority1ResolutionImplicits {

  def instance[In, Out](f: In => Out): Resolver[In,Out] = new Resolver[In,Out] {
    def apply(v1: In): Out = f(v1)
  }

  def nonResolver[E]: Resolver[E,E] = instance { v1 => v1 }

  /**Resolve a multiplication of a finite collection on the LHS with an inf mapping on the RHS.
    * This is the 'main event' of variable resolution, and works as follows:
    * 1 - Resolve the left hand side to get an expres
    * 2 - Require that the LHS evaluates to a finite collection.
    * 3 - Bind the key of the infinite mapping on the RHS (which may be a nested product of variables)
    *     to the discovered key type of the finite collection.
    * 4 - Recursively bind all instances of those variables in the value of the infinite mapping.
    * 5 - Recursively resolve the value of the infinite mapping.*/
  implicit def MultiplyInfResolver[
    LHS, LHS1, V, C, KT, VB,
    R1, R2, R3
  ](implicit resolveLeft: Resolver[LHS,LHS1], eval: Eval[LHS1,C], coll: Collection[C,KT,_],
             tagKey: Tagger[V,KT,V,VB], tagValue: Tagger[V,KT,R1,R2], resolveRight: Resolver[R2,R3]):
    Resolver[
      MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
      MultiplyExpr[LHS1,InfiniteMappingExpr[VB,R3]]
    ] =
    instance {
      case MultiplyExpr(l,InfiniteMappingExpr(k,r)) =>
        MultiplyExpr(
          resolveLeft(l),
          InfiniteMappingExpr(tagKey(k),resolveRight(tagValue(r)))
        )
    }

}


trait Priority1ResolutionImplicits  {
  /**Resolver base cases - primitive expressions and typed variables don't need to resolve to anything.
    * Note - there is no resolver for untyped variables - they are 'resolved' by being bound.*/
  implicit def LiteralResolver[V,ID]: Resolver[LiteralExpr[V,ID],LiteralExpr[V,ID]] =
    Resolver.nonResolver[LiteralExpr[V,ID]]

  implicit def TypedVariableResolver[T]: Resolver[TypedVariable[T],TypedVariable[T]] =
    Resolver.nonResolver[TypedVariable[T]]

  /**Standard inductive cases*/
  implicit def AddResolver[L, R, L1, R1]
  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[AddExpr[L,R],AddExpr[L1,R1]] =
    Resolver.instance { case AddExpr(l,r) => AddExpr(resolveL(l),resolveR(r)) }

  implicit def MultiplyResolver[L, R, L1, R1]
  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    Resolver.instance { case MultiplyExpr(l,r) => MultiplyExpr(resolveL(l),resolveR(r)) }

  implicit def JoinResolver[L, R, L1, R1]
  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[JoinExpr[L,R],JoinExpr[L1,R1]] =
    Resolver.instance { case JoinExpr(l,r) => JoinExpr(resolveL(l),resolveR(r)) }

  implicit def DotResolver[L, R, L1, R1]
  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[DotExpr[L,R],DotExpr[L1,R1]] =
    Resolver.instance { case DotExpr(l,r) => DotExpr(resolveL(l),resolveR(r)) }

  implicit def NotResolver[R1,R2]
  (implicit resolve: Resolver[R1,R2]): Resolver[NotExpr[R1],NotExpr[R2]] =
    Resolver.instance { case NotExpr(c) => NotExpr(resolve(c)) }

  implicit def NegateResolver[R1,R2]
  (implicit resolve: Resolver[R1,R2]): Resolver[NegateExpr[R1],NegateExpr[R2]] =
    Resolver.instance { case NegateExpr(c) => NegateExpr(resolve(c)) }

  implicit def SumResolver[R1,R2]
  (implicit resolve: Resolver[R1,R2]): Resolver[SumExpr[R1],SumExpr[R2]] =
    Resolver.instance { case SumExpr(c) => SumExpr(resolve(c)) }

  implicit def CollectResolver[R1,R2]
  (implicit resolve: Resolver[R1,R2]): Resolver[CollectExpr[R1],CollectExpr[R2]] =
    Resolver.instance { case CollectExpr(c) => CollectExpr(resolve(c)) }

  implicit def GroupResolver[R1,R2]
  (implicit resolve: Resolver[R1,R2]): Resolver[GroupExpr[R1],GroupExpr[R2]] =
    Resolver.instance { case GroupExpr(c) => GroupExpr(resolve(c)) }

  implicit def SngResolver[K,R,K1,R1]
  (implicit resolveK: Resolver[K,K1], resolveR: Resolver[R,R1]): Resolver[SngExpr[K,R],SngExpr[K1,R1]] =
    Resolver.instance { case SngExpr(k,r) => SngExpr(resolveK(k),resolveR(r)) }

//  implicit def PredicateResolver[L,R,L1,R1,T1,T2]
//  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[Predicate[L,R,T1,T2],Predicate[L1,R1,T1,T2]] =
//    Resolver.instance { case Predicate(l,r,f,opS) => Predicate(resolveL(l),resolveR(r),f,opS) }

  implicit def ApplyExprResolver[K1,K2,T,U]
  (implicit resolve: Resolver[K1,K2]): Resolver[ApplyExpr[K1,T,U],ApplyExpr[K2,T,U]] =
    Resolver.instance { case ApplyExpr(c,f) => ApplyExpr(resolve(c),f) }

  implicit def HNilResolver: Resolver[HNil,HNil] = Resolver.nonResolver[HNil]

  implicit def HListResolver[H1, T1 <: HList, H2, T2 <: HList]
  (implicit resolveH: Resolver[H1, H2], resolveT: Resolver[T1, T2]): Resolver[H1 :: T1, H2 :: T2] =
    Resolver.instance { case (h :: t) => resolveH(h) :: resolveT(t) }
}

trait ShreddedResolver[-In,+Out] extends (In => Out) with Serializable

object ShreddedResolver extends LowPriorityShreddedResolverImplicits {

  def instance[In, Out](f: In => Out): ShreddedResolver[In,Out] = new ShreddedResolver[In,Out] {
    def apply(v1: In): Out = f(v1)
  }

  def nonShreddedResolver[E]: ShreddedResolver[E,E] = instance { v1 => v1 }

  implicit def MultiplyInfResolver[
  LHS, LHS1, V, C, KT, VB,
  R1, R2, R3
  ](implicit resolveLeft: ShreddedResolver[LHS,LHS1], eval: ShreddedEval[LHS1,C,_], coll: Collection[C,KT,_],
    tagKey: Tagger[V,KT,V,VB], tagValue: Tagger[V,KT,R1,R2], resolveRight: ShreddedResolver[R2,R3]):
  ShreddedResolver[
    MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
    MultiplyExpr[LHS1,InfiniteMappingExpr[VB,R3]]
    ] =
    instance {
      case MultiplyExpr(l,InfiniteMappingExpr(k,r)) =>
        MultiplyExpr(
          resolveLeft(l),
          InfiniteMappingExpr(tagKey(k),resolveRight(tagValue(r)))
        )
    }
}

trait LowPriorityShreddedResolverImplicits {

  //todo - for some reason with just an Induct method here that took a normal resolver, this was being used
  //even in place of the specialised MultiplyinfShredded resolver, depsite attempts to control prioirty with scope.
  //thus for now, just leave it as duplicated code.
  /**Resolver base cases - primitive expressions and typed variables don't need to resolve to anything.
    * Note - there is no resolver for untyped variables - they are 'resolved' by being bound.*/
  implicit def LiteralShreddedResolver[V,ID]: ShreddedResolver[LiteralExpr[V,ID],LiteralExpr[V,ID]] =
    ShreddedResolver.nonShreddedResolver[LiteralExpr[V,ID]]

  implicit def TypedVariableShreddedResolver[T]: ShreddedResolver[TypedVariable[T],TypedVariable[T]] =
    ShreddedResolver.nonShreddedResolver[TypedVariable[T]]

  /**Standard inductive cases*/
  implicit def AddShreddedResolver[L, R, L1, R1]
  (implicit resolveL: ShreddedResolver[L,L1], resolveR: ShreddedResolver[R,R1]): ShreddedResolver[AddExpr[L,R],AddExpr[L1,R1]] =
    ShreddedResolver.instance { case AddExpr(l,r) => AddExpr(resolveL(l),resolveR(r)) }

  implicit def MultiplyShreddedResolver[L, R, L1, R1]
  (implicit resolveL: ShreddedResolver[L,L1], resolveR: ShreddedResolver[R,R1]): ShreddedResolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    ShreddedResolver.instance { case MultiplyExpr(l,r) => MultiplyExpr(resolveL(l),resolveR(r)) }

  implicit def JoinShreddedResolver[L, R, L1, R1]
  (implicit resolveL: ShreddedResolver[L,L1], resolveR: ShreddedResolver[R,R1]): ShreddedResolver[JoinExpr[L,R],JoinExpr[L1,R1]] =
    ShreddedResolver.instance { case JoinExpr(l,r) => JoinExpr(resolveL(l),resolveR(r)) }

  implicit def DotShreddedResolver[L, R, L1, R1]
  (implicit resolveL: ShreddedResolver[L,L1], resolveR: ShreddedResolver[R,R1]): ShreddedResolver[DotExpr[L,R],DotExpr[L1,R1]] =
    ShreddedResolver.instance { case DotExpr(l,r) => DotExpr(resolveL(l),resolveR(r)) }

  implicit def NotShreddedResolver[R1,R2]
  (implicit resolve: ShreddedResolver[R1,R2]): ShreddedResolver[NotExpr[R1],NotExpr[R2]] =
    ShreddedResolver.instance { case NotExpr(c) => NotExpr(resolve(c)) }

  implicit def NegateShreddedResolver[R1,R2]
  (implicit resolve: ShreddedResolver[R1,R2]): ShreddedResolver[NegateExpr[R1],NegateExpr[R2]] =
    ShreddedResolver.instance { case NegateExpr(c) => NegateExpr(resolve(c)) }

  implicit def SumShreddedResolver[R1,R2]
  (implicit resolve: ShreddedResolver[R1,R2]): ShreddedResolver[SumExpr[R1],SumExpr[R2]] =
    ShreddedResolver.instance { case SumExpr(c) => SumExpr(resolve(c)) }

  implicit def CollectShreddedResolver[R1,R2]
  (implicit resolve: ShreddedResolver[R1,R2]): ShreddedResolver[CollectExpr[R1],CollectExpr[R2]] =
    ShreddedResolver.instance { case CollectExpr(c) => CollectExpr(resolve(c)) }

  implicit def GroupShreddedResolver[R1,R2]
  (implicit resolve: ShreddedResolver[R1,R2]): ShreddedResolver[GroupExpr[R1],GroupExpr[R2]] =
    ShreddedResolver.instance { case GroupExpr(c) => GroupExpr(resolve(c)) }

  implicit def SngShreddedResolver[K,R,K1,R1]
  (implicit resolveK: ShreddedResolver[K,K1], resolveR: ShreddedResolver[R,R1]): ShreddedResolver[SngExpr[K,R],SngExpr[K1,R1]] =
    ShreddedResolver.instance { case SngExpr(k,r) => SngExpr(resolveK(k),resolveR(r)) }

//  implicit def PredicateShreddedResolver[L,R,L1,R1,T1,T2]
//  (implicit resolveL: ShreddedResolver[L,L1], resolveR: ShreddedResolver[R,R1]): ShreddedResolver[Predicate[L,R,T1,T2],Predicate[L1,R1,T1,T2]] =
//    ShreddedResolver.instance { case Predicate(l,r,f,opS) => Predicate(resolveL(l),resolveR(r),f,opS) }

  implicit def ApplyExprShreddedResolver[K1,K2,T,U]
  (implicit resolve: ShreddedResolver[K1,K2]): ShreddedResolver[ApplyExpr[K1,T,U],ApplyExpr[K2,T,U]] =
    ShreddedResolver.instance { case ApplyExpr(c,f) => ApplyExpr(resolve(c),f) }

  implicit def HNilShreddedResolver: ShreddedResolver[HNil,HNil] = ShreddedResolver.nonShreddedResolver[HNil]

  implicit def HListShreddedResolver[H1, T1 <: HList, H2, T2 <: HList]
  (implicit resolveH: ShreddedResolver[H1, H2], resolveT: ShreddedResolver[T1, T2]): ShreddedResolver[H1 :: T1, H2 :: T2] =
    ShreddedResolver.instance { case (h :: t) => resolveH(h) :: resolveT(t) }
}