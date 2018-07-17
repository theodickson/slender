/**Todo
  * Simplify priorities if possible.
  * Massive VariableNonTagger/VariableTagger appearance in logs
  */
package slender

import shapeless.{::, HList, HNil}

trait Tagger[V,T,-In,+Out] extends (In => Out) with Serializable

object Tagger extends Priority2TaggingImplicits {
  def instance[V,T,In,Out](f: In => Out): Tagger[V,T,In,Out] = new Tagger[V,T,In,Out] {
    def apply(v1: In): Out = f(v1)
  }
  def nonTagger[V,T,E]: Tagger[V,T,E,E] = instance { v1 => v1 }

  //Never try to get a tagger for an unused variable.
  implicit def UnusedTagger2[V, T]: Tagger[UnusedVariable, T, V, V] =
    Tagger.nonTagger[UnusedVariable, T, V]
}

trait Priority0TaggingImplicits {
  //The case that V1 <:< V and so actually tags to T takes precedence, hence this non-tager is here.
//  implicit def VariableNonTagger[V <: UntypedVariable,V1 <: UntypedVariable,T]: Tagger[V,T,V1,V1] =
//    Tagger.nonTagger[V,T,V1]

  implicit def VariableNonTagger[V,V1,T]:
    Tagger[UntypedVariable[V],T,UntypedVariable[V1],UntypedVariable[V1]] =
    Tagger.nonTagger[UntypedVariable[V],T,UntypedVariable[V1]]

  implicit def TagHList[V1, V2 <: HList, T1, T2 <: HList, E1, E2, E3]
  (implicit tag1: Tagger[V1,T1,E1,E2], tag2: Tagger[V2,T2,E2,E3]): Tagger[V1::V2, T1::T2, E1, E3] =
    Tagger.instance { v1 => tag2(tag1(v1)) }

  implicit def TagHNil[T,E]: Tagger[HNil,T,E,E] = Tagger.nonTagger[HNil,T,E]
}

trait Priority1TaggingImplicits extends Priority0TaggingImplicits {

  /** Standard inductive tagger cases. */
  implicit def AddTagger[V, T, L, R, L1, R1]
  (implicit tagL: Tagger[V, T, L, L1], tagR: Tagger[V, T, R, R1]): Tagger[V, T, AddExpr[L, R], AddExpr[L1, R1]] =
    Tagger.instance { case AddExpr(l,r) => AddExpr(tagL(l), tagR(r)) }

  implicit def MultiplyTagger[V, T, L, R, L1, R1]
  (implicit tagL: Tagger[V, T, L, L1], tagR: Tagger[V, T, R, R1]): Tagger[V, T, MultiplyExpr[L, R], MultiplyExpr[L1, R1]] =
    Tagger.instance { case MultiplyExpr(l,r) => MultiplyExpr(tagL(l), tagR(r)) }

  implicit def DotTagger[V, T, L, R, L1, R1]
  (implicit tagL: Tagger[V, T, L, L1], tagR: Tagger[V, T, R, R1]): Tagger[V, T, DotExpr[L, R], DotExpr[L1, R1]] =
    Tagger.instance { case DotExpr(l,r) => DotExpr(tagL(l), tagR(r)) }

  implicit def JoinTagger[V, T, L, R, L1, R1]
  (implicit tagL: Tagger[V, T, L, L1], tagR: Tagger[V, T, R, R1]): Tagger[V, T, JoinExpr[L, R], JoinExpr[L1, R1]] =
    Tagger.instance { case JoinExpr(l,r) => JoinExpr(tagL(l), tagR(r)) }

  implicit def NotTagger[V, T, R, R1]
  (implicit tag: Tagger[V, T, R, R1]): Tagger[V, T, NotExpr[R], NotExpr[R1]] =
    Tagger.instance { case NotExpr(c) => NotExpr(tag(c)) }

  implicit def NegateTagger[V, T, R, R1]
  (implicit tag: Tagger[V, T, R, R1]): Tagger[V, T, NegateExpr[R], NegateExpr[R1]] =
    Tagger.instance { case NegateExpr(c) => NegateExpr(tag(c)) }

  implicit def SumTagger[V, T, R, R1]
  (implicit tag: Tagger[V, T, R, R1]): Tagger[V, T, SumExpr[R], SumExpr[R1]] =
    Tagger.instance { case SumExpr(c) => SumExpr(tag(c)) }

  implicit def GroupTagger[V, T, R, R1]
  (implicit tag: Tagger[V, T, R, R1]): Tagger[V, T, GroupExpr[R], GroupExpr[R1]] =
    Tagger.instance { case GroupExpr(c) => GroupExpr(tag(c)) }

  implicit def SngTagger[V, T, K, R, K1, R1]
  (implicit tagK: Tagger[V, T, K, K1], tagR: Tagger[V, T, R, R1]): Tagger[V, T, SngExpr[K, R], SngExpr[K1, R1]] =
    Tagger.instance { case SngExpr(k,r) => SngExpr(tagK(k), tagR(r)) }

  implicit def ApplyExprTagger[V, T, R, R1, T1,U1]
  (implicit tag: Tagger[V, T, R, R1]): Tagger[V, T, ApplyExpr[R,T1,U1], ApplyExpr[R1,T1,U1]] =
    Tagger.instance { case ApplyExpr(c,f) => ApplyExpr(tag(c),f) }

  implicit def HNilTagger[V,T]: Tagger[V,T,HNil,HNil] = Tagger.nonTagger[V,T,HNil]

  implicit def HListTagger[V, T, H1, T1 <: HList, H2, T2 <: HList]
  (implicit tagH: Tagger[V,T,H1,H2], tagT: Tagger[V,T,T1,T2]): Tagger[V,T,H1 :: T1, H2 :: T2] =
    Tagger.instance { case (h1 :: t1) => tagH(h1) :: tagT(t1) }

}

trait Priority2TaggingImplicits extends Priority1TaggingImplicits {
  //Never try to tag typed variables.
  implicit def TypedNonTagger[V, T, T1]: Tagger[V, T, TypedVariable[T1], TypedVariable[T1]] =
    Tagger.nonTagger[V, T, TypedVariable[T1]]

  //Never try to tag unused variables.
  implicit def UnusedTagger1[V, T]: Tagger[V, T, UnusedVariable, UnusedVariable] =
    Tagger.nonTagger[V, T, UnusedVariable]

  /** Binding base cases - primitive expressions don't need to tag anything, variables tag iff they are untyped variables
    * matching the type in the tager. (otherwise they use the low priority non-taging case)
    */
  implicit def LiteralTagger[V, T, V1,ID]: Tagger[V, T, LiteralExpr[V1,ID], LiteralExpr[V1,ID]] =
    Tagger.nonTagger[V, T, LiteralExpr[V1,ID]]

  implicit def VariableTagger[V, T]: Tagger[UntypedVariable[V],T,UntypedVariable[V],TypedVariable[T]] =
    Tagger.instance { v1 => v1.tag[T] }

  /** Special tager for inner infinite mappings - dont attempt to tag the key */
  implicit def InfMappingTagger[V, T, K, R, R1]
  (implicit tagR: Tagger[V, T, R, R1]): Tagger[V, T, InfiniteMappingExpr[K, R], InfiniteMappingExpr[K, R1]] =
    Tagger.instance { case InfiniteMappingExpr(k,v) => InfiniteMappingExpr(k,tagR(v)) }
}

//  implicit def InductiveTagger[
//  V, T, E1,E2, Repr1 <: HList, Repr2 <: HList
//  ]
//  (implicit gen1: Generic.Aux[E1,Repr1], tag: Tagger[V, T, Repr1,Repr2],
//   ev: Reconstruct[E1, Repr2, E2], gen2: Generic.Aux[E2, Repr2]):
//  Tagger[V, T, E1, E2] = new Tagger[V, T, E1, E2] {
//    def apply(v1: E1): E2 = gen2.from(tag(gen1.to(v1)))
//  }
//
//  implicit def HNilTagger[V, T]: Tagger[V, T, HNil,HNil] =
//    new Tagger[V,T,HNil,HNil] {
//      def apply(v1: HNil) = v1
//    }
//
//  implicit def HListTagger[V, T, H1, T1 <: HList, H2, T2 <: HList]
//  (implicit tagH: Tagger[V, T, H1, H2], tagT: Tagger[V, T, T1, T2]):
//  Tagger[V,T,H1 :: T1, H2 :: T2] = new Tagger[V,T,H1 :: T1, H2 :: T2] {
//    def apply(v1: H1 :: T1) = v1 match {
//      case h1 :: t1 => tagH(h1) :: tagT(t1)
//    }
//  }

