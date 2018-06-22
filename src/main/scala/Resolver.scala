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

object Resolver extends Priority2ResolutionImplicits {

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
    LHS <: Expr, LHS1 <: Expr, V <: Expr, C[_,_], KT, VB <: Expr,
    RT, R1 <: Expr, R2 <: Expr, R3 <: Expr
  ](implicit resolveLeft: Resolver[LHS,LHS1], eval: Eval[LHS1,C[KT,RT]], coll: Collection[C,KT,RT],
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
//
//trait Priority0ResolutionImplicits {
//
//  implicit def NumericExprResolver[V]: Resolver[NumericExpr[V],NumericExpr[V]] =
//    Resolver.nonResolver[NumericExpr[V]]
//
//}

trait Priority1ResolutionImplicits  {
  /**Resolver base cases - primitive expressions and typed variables don't need to resolve to anything.
    * Note - there is no resolver for untyped variables - they are 'resolved' by being bound.*/
  implicit def LiteralResolver[V]: Resolver[LiteralExpr[V],LiteralExpr[V]] =
    Resolver.nonResolver[LiteralExpr[V]]

//  implicit def PhysicalCollectionResolver[C[_,_],K,R]: Resolver[PhysicalCollection[C,K,R],PhysicalCollection[C,K,R]] =
//    Resolver.nonResolver[PhysicalCollection[C,K,R]]

  implicit def VariableResolver[V <: UntypedVariable,T]: Resolver[TypedVariable[T],TypedVariable[T]] =
    Resolver.nonResolver[TypedVariable[T]]

}

trait Priority2ResolutionImplicits extends Priority1ResolutionImplicits {
  /**Standard inductive cases*/
  implicit def AddResolver[L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[AddExpr[L,R],AddExpr[L1,R1]] =
    Resolver.instance { case AddExpr(l,r) => AddExpr(resolveL(l),resolveR(r)) }

  implicit def MultiplyResolver[L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    Resolver.instance { case MultiplyExpr(l,r) => MultiplyExpr(resolveL(l),resolveR(r)) }

  implicit def JoinResolver[L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[JoinExpr[L,R],JoinExpr[L1,R1]] =
    Resolver.instance { case JoinExpr(l,r) => JoinExpr(resolveL(l),resolveR(r)) }

  implicit def DotResolver[L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[DotExpr[L,R],DotExpr[L1,R1]] =
    Resolver.instance { case DotExpr(l,r) => DotExpr(resolveL(l),resolveR(r)) }

  implicit def NotResolver[R1 <: Expr,R2 <: Expr]
  (implicit resolve: Resolver[R1,R2]): Resolver[NotExpr[R1],NotExpr[R2]] =
    Resolver.instance { case NotExpr(c) => NotExpr(resolve(c)) }

  implicit def NegateResolver[R1 <: Expr,R2 <: Expr]
  (implicit resolve: Resolver[R1,R2]): Resolver[NegateExpr[R1],NegateExpr[R2]] =
    Resolver.instance { case NegateExpr(c) => NegateExpr(resolve(c)) }

  implicit def SumResolver[R1 <: Expr,R2 <: Expr]
  (implicit resolve: Resolver[R1,R2]): Resolver[SumExpr[R1],SumExpr[R2]] =
    Resolver.instance { case SumExpr(c) => SumExpr(resolve(c)) }

  implicit def GroupResolver[R1 <: Expr,R2 <: Expr]
  (implicit resolve: Resolver[R1,R2]): Resolver[GroupExpr[R1],GroupExpr[R2]] =
    Resolver.instance { case GroupExpr(c) => GroupExpr(resolve(c)) }

  implicit def SngResolver[K <: Expr,R <: Expr,K1 <: Expr,R1 <: Expr]
  (implicit resolveK: Resolver[K,K1], resolveR: Resolver[R,R1]): Resolver[SngExpr[K,R],SngExpr[K1,R1]] =
    Resolver.instance { case SngExpr(k,r) => SngExpr(resolveK(k),resolveR(r)) }

  implicit def PredicateResolver[L <: Expr,R <: Expr,L1 <: Expr,R1 <: Expr,T1,T2]
  (implicit resolveL: Resolver[L,L1], resolveR: Resolver[R,R1]): Resolver[Predicate[L,R,T1,T2],Predicate[L1,R1,T1,T2]] =
    Resolver.instance { case Predicate(l,r,f,opS) => Predicate(resolveL(l),resolveR(r),f,opS) }

  implicit def ProductResolver[C <: HList, CR <: HList]
  (implicit resolve: Resolver[C,CR], lub: LUBConstraint[CR,Expr]): Resolver[ProductExpr[C],ProductExpr[CR]] =
    Resolver.instance { case ProductExpr(exprs) => ProductExpr(resolve(exprs)) }

  implicit def HNilResolver: Resolver[HNil,HNil] = Resolver.nonResolver[HNil]

  implicit def HListResolver[H1 <: Expr, T1 <: HList, H2 <: Expr, T2 <: HList]
  (implicit resolveH: Resolver[H1, H2], resolveT: Resolver[T1, T2]): Resolver[H1 :: T1, H2 :: T2] =
    Resolver.instance { case (h :: t) => resolveH(h) :: resolveT(t) }
}