package slender

trait Resolver[-In <: Expr,+Out <: Expr] extends (In => Out) with Serializable

trait Binder[V <: VariableExpr[V],T,-In <: Expr,+Out <: Expr] extends (In => Out) with Serializable

object Binder {
  def nonBinder[V <: VariableExpr[V], T, E <: Expr]: Binder[V,T,E,E] =
    new Binder[V,T,E,E] { def apply(v1: E) = v1 }
}

object Resolver {
  def nonResolver[E <: Expr]: Resolver[E,E] = new Resolver[E,E] { def apply(v1: E) = v1 }
}

trait LowPriorityVariableResolutionImplicits {
  implicit def VariableNonBinder[V <: VariableExpr[V],V1 <: VariableExpr[V1],T]: Binder[V,T,V1,V1] = Binder.nonBinder[V,T,V1]
  implicit def CantResolve[E <: Expr]: Resolver[E,E] = Resolver.nonResolver[E]
}

trait VariableResolutionImplicits extends LowPriorityVariableResolutionImplicits {

  /** Binding base cases - primitive expressions don't need to bind anything, variables bind iff they are untyped variables
    * matching the type in the binder. (otherwise they use the low priority non-binding case)
    */
  implicit def PrimitiveExprBinder[V <: VariableExpr[V],T,E <: PrimitiveExpr[_]]: Binder[V,T,E,E] =
    Binder.nonBinder[V,T,E]

  implicit def VariableBinder[V <: UntypedVariable[V],T]: Binder[V,T,V,TypedVariable[T]] =
    new Binder[V,T,V,TypedVariable[T]] { def apply(v1: V): TypedVariable[T] = v1.tag[T] }


  /**Special case for lifting two binders to a binder for a Tuple2Variable Expr - this lifted binder simply applies
    * the underlying binders in sequence.
    */
  implicit def BindTuple2[
    V1<:VariableExpr[V1],V2<:VariableExpr[V2],T1,T2,E1<:Expr,E2<:Expr,E3<:Expr
  ](implicit bind1: Binder[V1,T1,E1,E2],
             bind2: Binder[V2,T2,E2,E3]): Binder[Tuple2VariableExpr[V1,V2],(T1,T2),E1,E3] =
    new Binder[Tuple2VariableExpr[V1,V2],(T1,T2),E1,E3] {
      def apply(v1: E1) = bind2(bind1(v1))
    }

  implicit def BindTuple3[
    V1<:VariableExpr[V1],V2<:VariableExpr[V2],V3<:VariableExpr[V3],T1,T2,T3,E1<:Expr,E2<:Expr,E3<:Expr,E4<:Expr
  ](implicit bind1: Binder[V1,T1,E1,E2],
             bind2: Binder[V2,T2,E2,E3],
             bind3: Binder[V3,T3,E3,E4]): Binder[Tuple3VariableExpr[V1,V2,V3],(T1,T2,T3),E1,E4] =
    new Binder[Tuple3VariableExpr[V1,V2,V3],(T1,T2,T3),E1,E4] {
      def apply(v1: E1) = bind3(bind2(bind1(v1)))
    }

  /**Special binder for inner infinite mappings - dont attempt to bind the key*/
  implicit def InfMappingBinder[V <: UntypedVariable[V],T,K <: VariableExpr[K],R <: RingExpr,R1 <: RingExpr]
  (implicit bindR: Binder[V,T,R,R1]): Binder[V,T,InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,R1]] =
    new Binder[V,T,InfiniteMappingExpr[K,R],InfiniteMappingExpr[K,R1]] {
      def apply(v1: InfiniteMappingExpr[K,R]) = InfiniteMappingExpr(v1.key,bindR(v1.value))
    }


  /**Standard inductive binder cases.*/
  //This applies a single binder to a Tuple2 variable expression.
  implicit def Tuple2Binder[
  V <: UntypedVariable[V], T, V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], V1B <: VariableExpr[V1B], V2B <: VariableExpr[V2B]
  ](implicit bind1: Binder[V,T,V1,V1B], bind2: Binder[V,T,V2,V2B]):
  Binder[V,T,Tuple2VariableExpr[V1,V2],Tuple2VariableExpr[V1B,V2B]] =
    new Binder[V,T,Tuple2VariableExpr[V1,V2],Tuple2VariableExpr[V1B,V2B]] {
      def apply(v1: Tuple2VariableExpr[V1,V2]) = Tuple2VariableExpr(bind1(v1.c1), bind2(v1.c2))
    }

  implicit def Tuple3Binder[
    V <: UntypedVariable[V], T, V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], V3 <: VariableExpr[V3],
    V1B <: VariableExpr[V1B], V2B <: VariableExpr[V2B], V3B <: VariableExpr[V3B]
  ](implicit bind1: Binder[V,T,V1,V1B], bind2: Binder[V,T,V2,V2B], bind3: Binder[V,T,V3,V3B]):
  Binder[V,T,Tuple3VariableExpr[V1,V2,V3],Tuple3VariableExpr[V1B,V2B,V3B]] =
    new Binder[V,T,Tuple3VariableExpr[V1,V2,V3],Tuple3VariableExpr[V1B,V2B,V3B]] {
      def apply(v1: Tuple3VariableExpr[V1,V2,V3]) = Tuple3VariableExpr(bind1(v1.c1), bind2(v1.c2), bind3(v1.c3))
    }

  implicit def SumBinder[V <: UntypedVariable[V],T,R <: RingExpr,R1 <: RingExpr]
  (implicit recur: Binder[V,T,R,R1]): Binder[V,T,SumExpr[R],SumExpr[R1]] =
    new Binder[V,T,SumExpr[R],SumExpr[R1]] { def apply(v1: SumExpr[R]) = SumExpr(recur(v1.c1)) }

  implicit def SngBinder[V <: UntypedVariable[V],T,K <: KeyExpr,R <: RingExpr,K1 <: KeyExpr,R1 <: RingExpr]
  (implicit bindK: Binder[V,T,K,K1], bindR: Binder[V,T,R,R1]): Binder[V,T,SngExpr[K,R],SngExpr[K1,R1]] =
    new Binder[V,T,SngExpr[K,R],SngExpr[K1,R1]] { def apply(v1: SngExpr[K,R]) = SngExpr(bindK(v1.key),bindR(v1.value)) }

  implicit def MultiplyBinder[V <: UntypedVariable[V],T,L <: RingExpr,R <: RingExpr,L1 <: RingExpr,R1 <: RingExpr]
  (implicit bindL: Binder[V,T,L,L1], bindR: Binder[V,T,R,R1]): Binder[V,T,MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Binder[V,T,MultiplyExpr[L,R],MultiplyExpr[L1,R1]] { def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(bindL(v1.c1),bindR(v1.c2)) }


  /**Resolver base cases - primitive expressions and typed variables don't need to resolve to anything.
    * Note - there is no resolver for untyped variables - they are 'resolved' by being bound.*/
  implicit def PrimitiveExprResolver[E <: PrimitiveExpr[_]]: Resolver[E,E] = Resolver.nonResolver[E]

  implicit def VariableResolver[V <: UntypedVariable[V],T]: Resolver[TypedVariable[T],TypedVariable[T]] =
    Resolver.nonResolver[TypedVariable[T]]


  /**Resolve a multiplication of a finite collection on the LHS with an inf mapping on the RHS.
    * This is the 'main event' of variable resolution, and works as follows:
    * 1 - Resolve the LHS
    * 2 - Require that the LHS evaluates to a finite collection.
    * 3 - Bind the key of the infinite mapping on the RHS (which may be a nested product of variables)
    *     to the discovered key type of the finite collection.
    * 4 - Recursively bind all instances of those variables in the value of the infinite mapping.
    * 5 - Recursively resolve the value of the infinite mapping.*/
  implicit def MultiplyInfResolver[
    LHS <: RingExpr,LHS1<:RingExpr, V<:VariableExpr[V],C[_,_],KT,VB <: VariableExpr[VB],RT,R1<:RingExpr,R2<:RingExpr,R3<:RingExpr
  ](implicit resolveLeft: Resolver[LHS,LHS1], eval: Eval[LHS1,C[KT,RT]], coll: Collection[C,KT,RT],
             bindLeft: Binder[V,KT,V,VB], bindRight: Binder[V,KT,R1,R2], resolver: Resolver[R2,R3]):
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

  /**Standard inductive cases*/
  implicit def SumResolver[R1 <: RingExpr,R2 <: RingExpr](implicit resolver: Resolver[R1,R2]):
    Resolver[SumExpr[R1],SumExpr[R2]] = new Resolver[SumExpr[R1],SumExpr[R2]] {
      def apply(v1: SumExpr[R1]): SumExpr[R2] = SumExpr(resolver(v1.c1))
  }

  implicit def SngResolver[K <: KeyExpr,R <: RingExpr,K1 <: KeyExpr,R1 <: RingExpr]
  (implicit resolveK: Resolver[K,K1], resolveR: Resolver[R,R1]): Resolver[SngExpr[K,R],SngExpr[K1,R1]] =
    new Resolver[SngExpr[K,R],SngExpr[K1,R1]] { def apply(v1: SngExpr[K,R]) = SngExpr(resolveK(v1.key),resolveR(v1.value)) }


  implicit def MultiplyResolver[L <: RingExpr, R <: RingExpr, L1 <: RingExpr, R1 <: RingExpr]
  (implicit resolve1: Resolver[L,L1], resolve2: Resolver[R,R1]): Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] =
    new Resolver[MultiplyExpr[L,R],MultiplyExpr[L1,R1]] {
      def apply(v1: MultiplyExpr[L,R]) = MultiplyExpr(resolve1(v1.c1),resolve2(v1.c2))
    }

}
//
//
//object test {
//
//  import implicits._
//
//  def main(args: Array[String]): Unit = {
//    def getTag[T](implicit ev1: ClassTag[T]): ClassTag[T] = ev1
//    def getType[T](t: T)(implicit ev1: TypeTag[T]): TypeTag[T] = ev1
//    val bagOfInts = PhysicalCollection(Map(1 -> 1))
//    val bagOfIntPairs = PhysicalCollection(Map((1,2) -> 1))
//    val unevenTuples = PhysicalCollection(Map(((1,2),3) -> 1))
//
//    val query = For (X <-- bagOfInts) Collect IntExpr(2)
//      println(query.eval)
////    println()
////    println(query.eval)
////    val expr = For ((X,bagOfInts)) Collect IntExpr(1)
////    println(expr.eval)
////    def go[V <: VariableExpr[V]](v: V) = println("hi")
////    go(X)
//
//
//    //val tuple2maker = tuple2MakeVariableExpr[X,Y,X,Y]//(idMakeExpr[X],idMakeExpr[Y])
//
////    val expr2 = For ( Tuple2VariableExpr(X,Y) <-- bagOfIntPairs) Yield toExpr[(X,Y),Tuple2VariableExpr[X,Y]]((X,Y))
////    println(expr2.eval)
////    val expr3 = For (X <-- bagOfInts) Collect ( For (Y <-- bagOfInts) Yield X )
////    println(expr3.eval)
//
//  }
//}

