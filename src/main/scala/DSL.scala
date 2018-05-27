package slender

trait DSL {

//  implicit class RingExprOps[R <: RingExpr](expr: R) {
//
//    def +[R1 <: RingExpr](expr1: R1) = AddExpr(expr, expr1)
//
//    def *[R1 <: RingExpr](expr1: R1) = MultiplyExpr(expr, expr1)
//
//    def dot[R1 <: RingExpr](expr1: R1) = DotExpr(expr, expr1)
//
//    def sum = SumExpr(expr)
//
//    def unary_- = NegateExpr(expr)
//
//    def unary_! = NotExpr(expr)
//
//    //    def _1: RingExpr = ProjectRingExpr(e, 1)
//    //    def _2: RingExpr = ProjectRingExpr(e, 2)
//    //
//
//  }

//  implicit class KeyExprOps[K <: KeyExpr](val k: K) {
//    //      def _1: KeyExpr = ProjectKeyExpr(k, 1)
//    //      def _2: KeyExpr = ProjectKeyExpr(k, 2)
//    //      def _3: KeyExpr = ProjectKeyExpr(k, 3)
//
//    def ===[K1 <: KeyExpr](k1: K1) = EqualsPredicate(k, k1)
//
//    def =!=[K1 <: KeyExpr](k1: K1) = NotExpr(EqualsPredicate(k, k1))
//
//    def >[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ > _, ">")
//
//    def <[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ < _, "<")
//
//    def -->[R <: RingExpr](r: R): (K, R) = (k, r)
//  }

//
//  implicit class VariableExprOps[V <: VariableExpr](val v: V) {
//      def <--[R <: RingExpr](r: R): (V, R) = (v,r)
//      def ==>[R <: RingExpr,T](r: R): InfiniteMappingExpr[V,R] = InfiniteMappingExpr(v,r)
//  }
//  trait MakeRing
  trait MakeExpr[X,E <: Expr] extends (X => E)

  implicit def toExpr[X,E <: Expr](x: X)(implicit make: MakeExpr[X,E]): E = make(x)

//  implicit val XMakeExpr: MakeExpr[X,X] = idMakeExpr[X]
//
//  implicit val YMakeExpr: MakeExpr[Y,Y] = idMakeExpr[Y]
//
  implicit def idMakeExpr[E <: Expr]: MakeExpr[E,E] = new MakeExpr[E,E] { def apply(v1: E) = v1 }

//  implicit def idMakeVariableExpr[E <: VariableExpr[E]]: MakeExpr[E,E] = new MakeExpr[E,E] { def apply(v1: E) = v1 }

//  implicit def tuple2MakeRingExpr[X1,X2,R1 <: RingExpr, R2 <: RingExpr]
//    (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2RingExpr[R1,R2]] =
//    new MakeExpr[(X1,X2),Tuple2RingExpr[R1,R2]] {
//      def apply(v1: (X1,X2)) = Tuple2RingExpr(recur1(v1._1),recur2(v1._2))
//    }

  implicit def tuple2MakeVariableExpr[X1,X2,R1 <: VariableExpr[R1], R2 <: VariableExpr[R2]]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2VariableExpr[R1,R2]] =
    new MakeExpr[(X1,X2),Tuple2VariableExpr[R1,R2]] {
      def apply(v1: (X1,X2)) = Tuple2VariableExpr(recur1(v1._1),recur2(v1._2))
    }


  ////  implicit def pairMakeKeyExpr[A,B](implicit recur1: MakeKeyExpr[A],
    ////                                             recur2: MakeKeyExpr[B]): MakeKeyExpr[(A,B)]
    ////  = new MakeKeyExpr[(A,B)] {
    ////    def apply(x: (A,B)) =
    ////      KeyProductExpr(
    ////        List[KeyExpr](recur1(x._1), recur2(x._2))
    ////      )
    ////  }
    ////
    ////  implicit def pairMakeKeyExprOps[A,B](implicit recur1: MakeKeyExprOps[A],
    ////                                       recur2: MakeKeyExprOps[B]): MakeKeyExprOps[(A,B)]
    ////  = new MakeKeyExprOps[(A,B)] {
    ////    def apply(x: (A,B)) = KeyExprOps(
    ////      KeyProductExpr(
    ////        List[KeyExpr](recur1(x._1).k, recur2(x._2).k)
    ////      )
    ////    )
    ////  }
    ////
    ////  implicit def pairMakeVarKeyExprOps[A,B](implicit recur1: MakeVarKeyExprOps[A],
    ////                                       recur2: MakeVarKeyExprOps[B]): MakeVarKeyExprOps[(A,B)]
    ////    = new MakeVarKeyExprOps[(A,B)] {
    ////    def apply(x: (A,B)) = VarKeyExprOps(
    ////      ProductVariableKeyExpr(
    ////        List[VariableKeyExpr](recur1(x._1).k, recur2(x._2).k)
    ////      )
    ////    )
    ////  }
    ////
    ////  implicit def tripleMakeKeyExpr[A,B,C](implicit recur1: MakeKeyExpr[A],
    ////                                    recur2: MakeKeyExpr[B], recur3: MakeKeyExpr[C]): MakeKeyExpr[(A,B,C)]
    ////  = new MakeKeyExpr[(A,B,C)] {
    ////    def apply(x: (A,B,C)) =
    ////      KeyProductExpr(
    ////        List[KeyExpr](recur1(x._1), recur2(x._2), recur3(x._3))
    ////      )
    ////  }
    ////
    ////  implicit def tripleMakeKeyExprOps[A,B,C](implicit recur1: MakeKeyExprOps[A],
    ////                                       recur2: MakeKeyExprOps[B], recur3: MakeKeyExprOps[C]): MakeKeyExprOps[(A,B,C)]
    ////  = new MakeKeyExprOps[(A,B,C)] {
    ////    def apply(x: (A,B,C)) = KeyExprOps(
    ////      KeyProductExpr(
    ////        List[KeyExpr](recur1(x._1).k, recur2(x._2).k, recur3(x._3).k)
    ////      )
    ////    )
    ////  }
    ////
    ////  implicit def tripleMakeVarKeyExprOps[A,B,C](implicit recur1: MakeVarKeyExprOps[A],
    ////                                          recur2: MakeVarKeyExprOps[B], recur3: MakeVarKeyExprOps[C]): MakeVarKeyExprOps[(A,B,C)]
    ////  = new MakeVarKeyExprOps[(A,B,C)] {
    ////    def apply(x: (A,B,C)) = VarKeyExprOps(
    ////      ProductVariableKeyExpr(
    ////        List[VariableKeyExpr](recur1(x._1).k, recur2(x._2).k, recur3(x._3).k)
    ////      )
    ////    )
    ////  }
  ////  trait MakeKeyExprOps[X] extends (X => KeyExprOps)
  ////  trait MakeVarKeyExprOps[X] extends (X => VarKeyExprOps)

  implicit def sng[K <: KeyExpr, R <: RingExpr](k: K, r: R): SngExpr[K, R] = SngExpr(k, r)

  implicit def sng[K <: KeyExpr](k: K): SngExpr[K, IntExpr] = SngExpr(k, IntExpr(1))

  //
  //  def toK[O:Ring,E <: Expr[O,E]](e: E): BoxedRingExpr[O,E] = BoxedRingExpr(e)
  //
  //  def fromK[O:Ring,E <: Expr[O,E]](e: BoxedRingExpr[O,E]): E = e.c1
  //

  implicit def intToIntExpr(i: Int): IntExpr = IntExpr(i)

  case class ForComprehensionBuilder[V <: VariableExpr[V], R <: RingExpr](x: V, r1: R) {

    def Collect[R2 <: RingExpr, Out <: Expr]
      (r2: R2)
      (implicit resolver: Resolver[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, R2]]], Out]) =
        resolver(SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, r2))))

    def Yield[K <: KeyExpr, R2 <: RingExpr, Out <: Expr]
      (pair: (K, R2))
      (implicit resolver: Resolver[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, R2]]]], Out]): Out =
        Collect(sng(pair._1, pair._2))

    def Yield[K <: KeyExpr, Out <: Expr]
      (k: K)
      (implicit resolver: Resolver[SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, SngExpr[K, IntExpr]]]], Out]): Out =
        Yield(k, IntExpr(1))

  }

  object For {
    def apply[V <: VariableExpr[V], R <: RingExpr](paired: (V, R)): ForComprehensionBuilder[V, R] =
      ForComprehensionBuilder[V, R](paired._1, paired._2)
  }

}


////
////  implicit def boolToInt(b: Boolean): Int = if (b) 1 else 0
////
////  implicit class IffImplicit(pair: (VariableKeyExpr,RingExpr)) {
////    def iff(r1: RingExpr): (VariableKeyExpr,(RingExpr,RingExpr)) = (pair._1,(pair._2,r1))
////  }
////
////  implicit def intToIntKeyExpr(i: Int): PrimitiveKeyExpr[Int] = IntKeyExpr(i)
////  /**IntelliJ is not managing to resolve the conversions via the type-class mediator pattern even though its compiling
////    * so here are a bunch of explicit conversions to stop IDE errors. Hopefully can sort this out at some point.
////    * At time of writing everything compiles and tests run successfully with the below commented out so should
////    * periodically check that's still the case!
////    */
////
////
////  implicit def stringPairToVarKeyExpr(p: (String,String)): VariableKeyExpr =
////    ProductVariableKeyExpr(List(p._1, p._2))
////
////  implicit def stringPairToVarKeyExprOps(p: (String,String)): VarKeyExprOps =
////    VarKeyExprOps(ProductVariableKeyExpr(List(p._1, p._2)))
////
////  implicit def stringTripleToVarKeyExprOps(p: (String,String,String)): VarKeyExprOps =
////    VarKeyExprOps(ProductVariableKeyExpr(List(p._1, p._2, p._3)))
////
////  implicit def oddPair1(p: (String,KeyExpr)): KeyProductExpr =
////    KeyProductExpr(List(UntypedVariable(p._1),p._2))
////
////  implicit def oddPair2(p: (KeyExpr,String)): KeyProductExpr =
////    KeyProductExpr(List(p._1,UntypedVariable(p._2)))
////
////}
