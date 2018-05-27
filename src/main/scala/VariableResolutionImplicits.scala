package slender

import scala.reflect.{ClassTag,classTag}
import scala.reflect.runtime.universe._
import scala.collection.immutable.Map


trait Resolver[-In <: Expr,+Out <: Expr] extends (In => Out) with Serializable

//trait Binder[V <: UntypedVariableExpr[V],T,-In <: Expr,+Out <: Expr] extends (In => Out) with Serializable
trait Binder[V <: VariableExpr[_],T,-In <: Expr,+Out <: Expr] extends (In => Out) with Serializable

//trait VarBinder[V <: UntypedVariable[V],T,-In <: Expr,+Out <: Expr] extends Binder[V,T,In,Out]

//trait NonBinder[-V <: VariableExpr[_],T,In <: Expr] extends Binder[V,T,In,In] { def apply(v1: In) = v1 }
//trait NonBinder[V <: UntypedVariableExpr[V],T,In <: Expr] extends Binder[V,T,In,In] { def apply(v1: In) = v1 }

object Binder {
//  implicit def VariableNonBinder[V <: VariableExpr[_],V1 <: VariableExpr[_],T] = new NonBinder[V,T,V1] { }
//  implicit def VariableNonBinder[V <: UntypedVariable[V],V1 <: UntypedVariable[V1],T] = new NonBinder[V,T,V1] { }

  def nonBinder[V <: VariableExpr[_], T, E <: Expr]: Binder[V,T,E,E] =
    new Binder[V,T,E,E] { def apply(v1: E) = v1 }

}

object Resolver {
  def nonResolver[E <: Expr]: Resolver[E,E] = new Resolver[E,E] { def apply(v1: E) = v1 }
}

//trait CanFoo[A] extends (A => String)
//
//trait Implicits1 {
//  implicit def CanFoo1[A]: CanFoo[A] = new CanFoo[A] { def apply(a: A) = "1 can foo" }
//}
//
//trait Implicits2 extends Implicits1 {
//  implicit def CanFoo2[A]: CanFoo[A] = new CanFoo[A] { def apply(a: A) = "2 can foo" }
//}

trait Priority1VariableResolutionImplicits {
  implicit def VariableNonBinder[V <: Variable[_,_],V1 <: Variable[_,_],T]: Binder[V,T,V1,V1] = Binder.nonBinder[V,T,V1]
}

trait Priority2VariableResolutionImplicits extends Priority1VariableResolutionImplicits {

  //create a binder for

//  implicit def DownTuple2Binder[
//    V <: VariableExpr[_],V1 <: Tuple2VariableExpr[_,_,_,_],VB1 <: Tuple2VariableExpr[_,_,_,_],
//    VB2 <: Tuple2VariableExpr[_,_,_,_],T//,T1,T2
//  ](implicit bind1: Binder[V,T,V1,VB1], bind2: Binder[V,T,VB1,VB2]): Binder[V,T,V1,VB2] = new Binder[V,T,V1,VB2] {
//    def apply(v1: V1) = bind2(bind1(v1))
//  }

//  implicit def DownTuple2Binder[
//    V <: VariableExpr[_],V1 <: VariableExpr[_],V1B <: VariableExpr[_],V2 <: VariableExpr[_],V2B <: VariableExpr[_],T,T1,T1B,T2,T2B
//  ](implicit bind1: Binder[V,T,V1,V1B], bind2: Binder[V,T,V2,V2B],
//    ev1: V1 <:< VariableExpr[T1],ev1b: V1B <:< VariableExpr[T1B],ev2: V2 <:< VariableExpr[T2],ev2b: V2B <:< VariableExpr[V2B]):
//    Binder[V,T,Tuple2VariableExpr[V1,V2,_,_],Tuple2VariableExpr[V1B,V2B,_,_]] =
//      new Binder[V,T,Tuple2VariableExpr[V1,V2,_,_],Tuple2VariableExpr[V1B,V2B,_,_]] {
//    def apply(v1: Tuple2VariableExpr[V1,V2,_,_]) = Tuple2VariableExpr(bind1(v1.c1),bind2(v1.c2))
//  }


  implicit def UntypedTuple2Binder[
    V <: UntypedVariable[V], T, V1 <: Variable[_,_], V2 <: Variable[_,_], V1B <: Variable[_,_], V2B <: Variable[_,_],T1,T1B,T2,T2B
  ](implicit bind1: Binder[V,T,V1,V1B], bind2: Binder[V,T,V2,V2B],
    ev1: V1 <:< VariableExpr[T1],ev1b: V1B <:< VariableExpr[T1B],ev2: V2 <:< VariableExpr[T2],ev2b: V2B <:< VariableExpr[T2B]):
    Binder[V,T,Tuple2VariableExpr[V1,V2,T1,T2],Tuple2VariableExpr[V1B,V2B,T1B,T2B]] =
    new Binder[V,T,Tuple2VariableExpr[V1,V2,T1,T2],Tuple2VariableExpr[V1B,V2B,T1B,T2B]] {
      def apply(v1: Tuple2VariableExpr[V1,V2,T1,T2]) = Tuple2VariableExpr(bind1(v1.c1), bind2(v1.c2))
    }
//  implicit def UpTuple2Binder[
//  V1<:VariableExpr[_], V2<:VariableExpr[_],T1,T2,E1<:Expr,E2<:Expr,E3<:Expr
//  ](implicit bind1: Binder[V1,T1,E1,E2],
//    bind2: Binder[V2,T2,E2,E3]): Binder[Tuple2VariableExpr[V1,V2,T1,T2],(T1,T2),E1,E3] =
//    new Binder[Tuple2VariableExpr[V1,V2,T1,T2],(T1,T2),E1,E3] {
//      def apply(v1: E1) = bind2(bind1(v1))
//    }

  implicit def BindUntypedTuple2[
    V1<:UntypedVariable[V1], V2<:UntypedVariable[V2],T1,T2,E1<:Expr,E2<:Expr,E3<:Expr
  ](implicit bind1: Binder[V1,T1,E1,E2],
    bind2: Binder[V2,T2,E2,E3]): Binder[Tuple2VariableExpr[V1,V2,T1,T2],(T1,T2),E1,E3] =
    new Binder[Tuple2VariableExpr[V1,V2,T1,T2],(T1,T2),E1,E3] {
      def apply(v1: E1) = bind2(bind1(v1))
    }
}

trait VariableResolutionImplicits extends Priority2VariableResolutionImplicits {

  def resolve[In <: Expr, Out <: Expr](in: In)(implicit resolver: Resolver[In,Out]): Out = resolver(in)

  def bind[V <: UntypedVariableExpr[V],T,In<:Expr,Out<:Expr](in: In)(implicit binder: Binder[V,T,In,Out]): Out = binder(in)

  /** Base cases - primitive expressions and variables resolve to themselves, prim exprs bind to themselves, var */
  implicit def PrimitiveExprResolver[E <: PrimitiveExpr[_]]: Resolver[E,E] = Resolver.nonResolver[E]

  implicit def VariableResolver[V <: UntypedVariable[V],T]: Resolver[TypedVariable[V,T],TypedVariable[V,T]] =
    Resolver.nonResolver[TypedVariable[V,T]]
//
  implicit def PrimitiveExprBinder[V <: UntypedVariable[V],T,E <: PrimitiveExpr[_]]: Binder[V,T,E,E] =
    Binder.nonBinder[V,T,E]

  implicit def VariableBinder[V <: UntypedVariable[V],T]: Binder[V,T,V,TypedVariable[V,T]] =
    new Binder[V,T,V,TypedVariable[V,T]] { def apply(v1: V): TypedVariable[V,T] = v1.tag[T] }


  //lift two binders to a tuple2 binder to apply downwards
//  implicit def UpTuple2Binder[
//    V1<:UntypedVariableExpr[V1], V2<:UntypedVariableExpr[V2],T1,T2,E1<:Expr,E2<:Expr,E3<:Expr
//  ](implicit bind1: Binder[V1,T1,E1,E2],
//             bind2: Binder[V2,T2,E2,E3]): Binder[Tuple2UntypedVariableExpr[V1,V2],(T1,T2),E1,E3] =
//    new Binder[Tuple2UntypedVariableExpr[V1,V2],(T1,T2),E1,E3] {
//      def apply(v1: E1) = bind2(bind1(v1))
//    }


  //todo - need binders which apply a binder to a tupled variable expression
  //the above cant find the lower binders because they need to

  /**Special multiply w/ inf mapping resolver*/
  implicit def MultiplyInfResolver[
    LHS <: RingExpr,LHS1<:RingExpr, V<:UntypedVariableExpr[V],C[_,_],KT,VB <: VariableExpr[_],RT,R1<:RingExpr,R2<:RingExpr,R3<:RingExpr
  ](implicit resolveLeft: Resolver[LHS,LHS1], eval: Eval[LHS1,C[KT,RT]], coll: Collection[C,KT,RT],
             bindLeft: Binder[V,KT,V,VB], bindRight: Binder[V,KT,R1,R2], resolver: Resolver[R2,R3], ev: VB <:< VariableExpr[KT]):
    Resolver[
      MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
      MultiplyExpr[LHS1,InfiniteMappingExpr[VB,R3]]
    ] =
    new Resolver[
      MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
      MultiplyExpr[LHS1,InfiniteMappingExpr[VB,R3]]
      ] {
      def apply(v1: MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]]) =
        MultiplyExpr(resolveLeft(v1.c1),InfiniteMappingExpr(bindLeft(v1.c2.key),resolver(bindRight(v1.c2.value))))
    }

//  /**Special multiply w/ inf mapping resolver*/
//  implicit def MultiplyInfResolver[
//  LHS <: RingExpr,LHS1<:RingExpr, V<:UntypedVariable[V],C[_,_],KT,RT,R1<:RingExpr,R2<:RingExpr,R3<:RingExpr
//  ](implicit resolveLeft: Resolver[LHS,LHS1], eval: Eval[LHS1,C[KT,RT]], coll: Collection[C,KT,RT],
//    bindLeft: Binder[V,KT,V,TypedVariable[V,KT]], bindRight: Binder[V,KT,R1,R2], resolver: Resolver[R2,R3]):
//  Resolver[
//    MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
//    MultiplyExpr[LHS1,InfiniteMappingExpr[TypedVariable[V,KT],R3]]
//    ] =
//    new Resolver[
//      MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]],
//      MultiplyExpr[LHS1,InfiniteMappingExpr[TypedVariable[V,KT],R3]]
//      ] {
//      def apply(v1: MultiplyExpr[LHS,InfiniteMappingExpr[V,R1]]) =
//        MultiplyExpr(resolveLeft(v1.c1),InfiniteMappingExpr(bindLeft(v1.c2.key),resolver(bindRight(v1.c2.value))))
//    }
//
//  def multiplyInfResolverAux[LHS <: RingExpr,V <: UntypedVariable[V],C[_,_],KT,RT,R1<:RingExpr,R2<:RingExpr,R3<:RingExpr]
//  (eval: Eval[LHS,C[KT,RT]], resolver: Resolver[R2,R3])(implicit coll: Collection[C,KT,RT],binder: Binder[V,KT,R1,R2],ev: ClassTag[KT]) =
//    MultiplyInfResolver(ev,eval,coll,binder,resolver)
//
//  def multiplyInfResolverAux2[LHS <: RingExpr,V <: UntypedVariable[V],C[_,_],KT,RT,R1<:RingExpr,R2<:RingExpr,R3<:RingExpr]
//  (coll: Collection[C,KT,RT])(implicit eval: Eval[LHS,C[KT,RT]], binder: Binder[V,KT,R1,R2], resolver: Resolver[R2,R3],ev: ClassTag[KT]) =
//    MultiplyInfResolver(ev,eval,coll,binder,resolver)
//
//  def multiplyInfResolverAux3[LHS <: RingExpr,V <: UntypedVariable[V],C[_,_],KT,RT,R1<:RingExpr,R2<:RingExpr,R3<:RingExpr](implicit coll: Collection[C,KT,RT], eval: Eval[LHS,C[KT,RT]], binder: Binder[V,KT,R1,R2], resolver: Resolver[R2,R3],ev: ClassTag[KT]) =
//    MultiplyInfResolver(ev,eval,coll,binder,resolver)

  /**Special binder for inner infinite mappings - dont attempt to bind the key*/
  implicit def InfMappingBinder[V <: UntypedVariable[V],T,K <: VariableExpr[_],R <: RingExpr,R1 <: RingExpr]
  (implicit bindR: Binder[V,T,R,R1]): Binder[V,T,InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,R1]] =
    new Binder[V,T,InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,R1]] {
      def apply(v1: InfiniteMappingExpr[K,R]) = InfiniteMappingExpr(v1.key,bindR(v1.value))
    }

  /**Standard inductive cases*/
  implicit def SumResolver[R1 <: RingExpr,R2 <: RingExpr](implicit resolver: Resolver[R1,R2]):
    Resolver[SumExpr[R1],SumExpr[R2]] = new Resolver[SumExpr[R1],SumExpr[R2]] {
      def apply(v1: SumExpr[R1]): SumExpr[R2] = SumExpr(resolver(v1.c1))
  }

  implicit def SumBinder[V <: UntypedVariable[V],T,R <: RingExpr,R1 <: RingExpr]
  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,SumExpr[R],SumExpr[R1]] =
    new Binder[V,T,SumExpr[R],SumExpr[R1]] { def apply(v1: SumExpr[R]) = SumExpr(recur(v1.c1)) }

  implicit def SngResolver[K <: KeyExpr,R <: RingExpr,K1 <: KeyExpr,R1 <: RingExpr]
  (implicit resolveK: Resolver[K,K1], resolveR: Resolver[R,R1]): Resolver[SngExpr[K,R],SngExpr[K1,R1]] =
    new Resolver[SngExpr[K,R],SngExpr[K1,R1]] { def apply(v1: SngExpr[K,R]) = SngExpr(resolveK(v1.key),resolveR(v1.value)) }

  implicit def SngBinder[V <: UntypedVariable[V],T,K <: KeyExpr,R <: RingExpr,K1 <: KeyExpr,R1 <: RingExpr]
  (implicit bindK: Binder[V,T,K,K1], bindR: Binder[V,T,R,R1]): Binder[V,T,SngExpr[K,R],SngExpr[K1,R1]] =
    new Binder[V,T,SngExpr[K,R],SngExpr[K1,R1]] { def apply(v1: SngExpr[K,R]) = SngExpr(bindK(v1.key),bindR(v1.value)) }

  implicit def MultiplyResolver[L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] {
      def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

  implicit def MultiplyBinder[V <: UntypedVariable[V],T,L <: RingExpr,R <: RingExpr,L1 <: RingExpr,R1 <: RingExpr]
  (implicit bindL: Binder[V,T,L,L1], bindR: Binder[V,T,R,R1]): Binder[V,T,MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Binder[V,T,MultiplyExpr[L,R],MultiplyExpr[L1,R1]] { def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(bindL(v1.c1),bindR(v1.c2)) }

}

//
object test extends VariableResolutionImplicits {

  def main(args: Array[String]): Unit = {
    def getTag[T](implicit ev1: ClassTag[T]): ClassTag[T] = ev1
    def getType[T](t: T)(implicit ev1: TypeTag[T]): TypeTag[T] = ev1
    val bagOfInts = PhysicalCollection(Map(1 -> 1))
    val bagOfIntPairs = PhysicalCollection(Map((1,2) -> 1))


//    val expr = For (X <-- bagOfInts) Collect IntExpr(1)
//    println(resolve(expr))

//    val expr2 = For (X <-- bagOfInts) Yield X
//    println(resolve(expr2))
//    val tupleVarExpr = Tuple2UntypedVariableExpr[X,Y](X,Y)
//    val myBinder = selfBind(tupleVarExpr, (1,1))
//    println(getType(myBinder))
    val expr3 = For ((Tuple2VariableExpr(X,Y),bagOfIntPairs)) Collect IntExpr(1)
    println(getType(resolve(expr3)))
//      def foo[A](a: A)(implicit doFoo: CanFoo[A]): Unit = println(doFoo(a))
//
//      foo(1)
//    println(getType(resolve(expr3)))
//    println(getType(expr3))
//    println(getType(
//      Tuple2VariableExpr(TypedVariable[X,Int](X),Y)
//    ))

//    println(expr3.eval)

//    val inner = For (Y <-- bagOfInts) Yield
//    val outer = For (X <-- bagOfInts) Collect inner
//    val expr = For (X <-- bagOfInts) Yield (X, For (Y <-- bagOfInts) Yield Y )
//    println(resolve(expr).eval)
      //val expr = For (X <-- bagOfInts) Yield ((X, For (Y <-- bagOfInts) Yield Y) )
//
//    println(getType(inner))
//    println(getType(outer))
////    println(getType(InfiniteMappingExpr(X,IntExpr(1))))
//    println(getType(
//      SumExpr(MultiplyExpr(bagOfInts,InfiniteMappingExpr(X,IntExpr(1))))
//    ))
//    println(getType(
//      For (X <-- bagOfInts) Collect IntExpr(1)
//    ))
//    println(getType(For (X <-- bagOfInts)))
//    println(getType(SngExpr(X,IntExpr(1))))
//    println(getType(resolve(outer)))

//    println(resolve(MultiplyExpr(multExprUnresolved,multExprUnresolved)).eval)
//    println(expr2.eval)
//    println(resolve(expr))
//    println(resolve(expr).eval)
//    println(getType(multExprUnresolved))
//    println(getType(tag(multExprUnresolved)))
//    val resolved = resolve(infMapping)

//    println(infMapping)
//    println(resolve(infMapping))
//    val infMapping = InfiniteMappingExpr[X,Nothing,IntExpr](X,IntExpr(1))
//    val multExpr = MultiplyExpr(PhysicalCollection(bagOfInts),infMapping)

//    implicit val resolver = MultiplyInfResolver[PhysicalCollection[Map,Int,Int],X,Map,Int,Int,IntExpr,IntExpr,IntExpr](
//      getTag[Int],PrimitiveExprEval[PhysicalCollection[Map,Int,Int],Map[Int,Int]],mapToCollection[Int,Int],PrimitiveExprBinder[X,Int,IntExpr],
//      PrimitiveExprResolver[IntExpr]
//    )
//
//     val innerResolver = PrimitiveExprResolver[IntExpr]
//     val binder = PrimitiveExprBinder[X,Int,IntExpr]
//     implicit val coll = mapToCollection[Int,Int]
//     implicit val eval = PrimitiveExprEval[PhysicalCollection[Map,Int,Int],Map[Int,Int]]
//      implicit val collEval = toCollectionEval[Map,Int,Int]
//    implicit val tag = getTag[Int]
//    val resolver = multiplyInfResolverAux(eval,innerResolver)

//    println(getType(resolver))
//    val resolver2 = multiplyInfResolverAux2(coll)
//    val resolver3 = multiplyInfResolverAux3
//    println(resolve(multExpr))
//      val expr = For (X <-- bagOfInts) Collect IntExpr(1)
//      implicit val
//    println(resolve(expr).eval)
//    val multExpr = MultiplyExpr(bagOfInts,infMapping)
//    println(infMapping)
//    println(multExpr)
//    val taggedMult = tag(multExpr)//(MultiplySingleTagger[]))
//    val resolvedInner = resolve(taggedMult.c2)
//    println(resolvedInner)
//    val resolved = resolve(tag(multExpr))
//    println(resolved)
//    println(taggedMult.getClass)
//    println(resolvedMult)
//    println(bind[X,Int,X,TypedVariable[X,Int]](X))
//    println(bind[X,Int,X,X](X))



  }
}
//
