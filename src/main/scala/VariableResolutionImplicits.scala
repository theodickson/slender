package slender

import scala.reflect.ClassTag


//Generically resolves any expression
trait Resolver[In <: Expr,Out <: Expr] extends (In => Out) with Serializable

trait NonResolver[In <: Expr] extends Resolver[In,In] { def apply(v1: In) = v1}

//Recursively binds all instanced of V to Variable[V,T] in the InExpr
trait Binder[V <: UntypedVariable[V],T,In <: Expr,Out <: Expr] extends (In => Out) with Serializable

trait NonBinder[V <: UntypedVariable[V],T,In <: Expr] extends Binder[V,T,In,In] { def apply(v1: In) = v1 }

//Tags the Variables on the LHS of all outermost infinite mappings
trait Tagger[In <: Expr,Out <: Expr] extends (In => Out) with Serializable

trait NonTagger[In <: Expr] extends Tagger[In,In] { def apply(v1: In) = v1 }

object Binder {
  implicit def VariableNonBinder[V <: UntypedVariable[V],V1 <: UntypedVariable[V1],T] =
    new Binder[V1,T,V,V] { def apply(v1: V): V = v1 }
}

trait VariableResolutionImplicits {

  def resolve[In <: Expr, Out <: Expr](in: In)(implicit resolver: Resolver[In,Out]): Out = resolver(in)

  def bind[V <: UntypedVariable[V],T,In<:Expr,Out<:Expr](in: In)(implicit binder: Binder[V,T,In,Out]): Out = binder(in)

  def tag[In <: Expr, Out <: Expr](in: In)(implicit tagger: Tagger[In,Out]): Out = tagger(in)

  implicit def PrimitiveExprResolver[E <: PrimitiveExpr[_]]: NonResolver[E] = new NonResolver[E] {}

  implicit def PrimitiveExprTagger[E <: PrimitiveExpr[_]]: NonTagger[E] = new NonTagger[E] {}

  implicit def PrimitiveExprBinder[V <: UntypedVariable[V],T,E <: PrimitiveExpr[_]]: NonBinder[V,T,E] =
    new NonBinder[V,T,E] {}


  implicit def SingleMultiplyTagger[LHS <: RingExpr,V <: UntypedVariable[V],R <: RingExpr,C[_,_],KT : ClassTag,RT]
  (implicit eval: Eval[LHS,C[KT,RT]], coll: Collection[C,KT,RT]): Tagger[
      MultiplyExpr[LHS,InfiniteMappingExpr[V,Nothing,R]],
      MultiplyExpr[LHS,InfiniteMappingExpr[TypedVariable[V,KT],KT,R]]
        ] =
    new Tagger[
      MultiplyExpr[LHS,InfiniteMappingExpr[V,Nothing,R]],
      MultiplyExpr[LHS,InfiniteMappingExpr[TypedVariable[V,KT],KT,R]]
        ] {
        def apply(v1: MultiplyExpr[LHS,InfiniteMappingExpr[V,Nothing,R]]) =
          MultiplyExpr(v1.c1,InfiniteMappingExpr(v1.c2.key.tag[KT],v1.c2.value))
      }

  implicit def VariableBinder[V <: UntypedVariable[V],T : ClassTag]: Binder[V,T,V,TypedVariable[V,T]] =
    new Binder[V,T,V,TypedVariable[V,T]] { def apply(v1: V): TypedVariable[V,T] = v1.tag[T] }


  implicit def SingleInfiniteMappingResolver[
    V <: UntypedVariable[V],KT,RHS1 <: RingExpr,RHS2 <: RingExpr,RHS3 <: RingExpr,RHS4 <: RingExpr
  ](implicit binder: Binder[V,KT,RHS1,RHS2], tagger: Tagger[RHS2,RHS3], resolver: Resolver[RHS3,RHS4]):
  Resolver[InfiniteMappingExpr[TypedVariable[X,KT],KT,RHS1],InfiniteMappingExpr[TypedVariable[X,KT],KT,RHS4]] =
    new Resolver[InfiniteMappingExpr[TypedVariable[X,KT],KT,RHS1],InfiniteMappingExpr[TypedVariable[X,KT],KT,RHS4]] {
      def apply(v1: InfiniteMappingExpr[TypedVariable[X,KT],KT,RHS1]): InfiniteMappingExpr[TypedVariable[X,KT],KT,RHS4] =
        InfiniteMappingExpr(v1.c1,resolver(tagger(binder(v1.c2))))
    }

  implicit def MultiplyResolver[L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] {
      def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

}
//
object test extends VariableResolutionImplicits {

  def main(args: Array[String]): Unit = {
    val bagOfInts = Map(1 -> 1)
    val infMapping = InfiniteMappingExpr[X,Nothing,IntExpr](X,IntExpr(1))
    val multExpr = MultiplyExpr(PhysicalCollection(bagOfInts),infMapping)
//    val multExpr = MultiplyExpr(bagOfInts,infMapping)
    println(infMapping)
    println(multExpr)
    val taggedMult = tag(multExpr)//(MultiplySingleTagger[]))
    val resolvedInner = resolve(taggedMult.c2)
    println(resolvedInner)
    val resolved = resolve(tag(multExpr))
    println(resolved)
//    println(taggedMult.getClass)
//    println(resolvedMult)
//    println(bind[X,Int,X,TypedVariable[X,Int]](X))
//    println(bind[X,Int,X,X](X))



  }
}
//