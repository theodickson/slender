package slender

import org.apache.spark.rdd.RDD
import shapeless.ops.hlist.ToTraversable
import shapeless._
import shapeless.syntax._

import scala.reflect.runtime.universe._

case class VariableRingPredicate[V <: Expr, R <: Expr, P <: Expr](k: V, r: R, p: P = NumericExpr(1))
case class KeyRingPair[K <: Expr, R <: Expr](k: K, r: R)

case class ExprOps[E <: Expr](e: E) {

  def eval[R <: Expr,T](implicit resolve: Resolver[E,R], evaluator: Eval[R,T]): T = evaluator(resolve(e),Map.empty)
  def evalType[T : TypeTag, R <: Expr](implicit resolve: Resolver[E,R], evaluator: Eval[R,T]): Type = typeTag[T].tpe
  def resolve[T <: Expr](implicit resolver: Resolver[E,T]): T = resolver(e)
  def shred[Shredded <: Expr](implicit shredder: Shredder[E,Shredded]): Shredded = shredder(e)
  def shreddable[Shredded <: Expr](implicit canShred: Perhaps[Shredder[E,Shredded]]) = canShred.value.isDefined
  def isEvaluable[T](implicit canEval: Perhaps[Eval[E,_]]) = canEval.value.isDefined
  def isResolvable[T](implicit canResolve: Perhaps[Resolver[E,_]]): Boolean = canResolve.value.isDefined

  def ===[K1 <: Expr](k1: K1) = EqualsPredicate(e, k1)
  def =!=[K1 <: Expr](k1: K1) = NotExpr(EqualsPredicate(e, k1))
  def >[K1 <: Expr](k1: K1) = IntPredicate(e, k1, _ > _, ">")
  def <[K1 <: Expr](k1: K1) = IntPredicate(e, k1, _ < _, "<")
  def -->[R <: Expr](r: R): KeyRingPair[E,R] = KeyRingPair(e,r)

  def +[R1 <: Expr](expr1: R1) = AddExpr(e,expr1)
  def *[R1 <: Expr](expr1: R1) = MultiplyExpr(e,expr1)
  def dot[R1 <: Expr](expr1: R1) = DotExpr(e,expr1)
  def join[R1 <: Expr](expr1: R1) = JoinExpr(e,expr1)
  def sum = SumExpr(e)
  def unary_- = NegateExpr(e)
  def unary_! = NotExpr(e)
  def &&[R1 <: Expr](expr1: R1) = this.*[R1](expr1)
  def ||[R1 <: Expr](expr1: R1) = this.+[R1](expr1)

  def <--[R <: Expr](r: R): VariableRingPredicate[E,R,NumericExpr[Int]] = VariableRingPredicate(e,r)
  def ==>[R <: Expr](r: R): InfiniteMappingExpr[E,R] = InfiniteMappingExpr(e,r)
}

case class ForComprehensionBuilder[V <: Expr, R <: Expr, P <: Expr](vrp: VariableRingPredicate[V,R,P]) {
  //todo - get rid of default NumericExpr(1)

  val x = vrp.k; val r1 = vrp.r; val p = vrp.p

  def _collect[R2 <: Expr](r2: R2): SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, DotExpr[R2,P]]]] =
    SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, DotExpr(r2,p))))

  def Collect[T, R2 <: Expr]
  (r2: T)
  (implicit make: MakeExpr[T,R2]) = _collect(make(r2))

  def Yield[T, K <: Expr, R2 <: Expr]
  (x: T)
  (implicit make: MakeKeyRingPair[T,K,R2]) = {
    val made = make(x)
    _collect(SngExpr(made.k, made.r))
  }
}

case class NestedForComprehensionBuilder[
V1 <: Expr, V2 <: Expr, R1 <: Expr, R2 <: Expr, P1 <: Expr, P2 <: Expr
](builder1: ForComprehensionBuilder[V1,R1,P1], builder2: ForComprehensionBuilder[V2,R2,P2]) {
  //todo - this works but takes forever to compile. Perhaps a more elegant and general solution might be quicker,
  //otherwise can it.
  def Collect[T, R3 <: Expr]
  (r3: T)(implicit make: MakeExpr[T, R3]) = {
    val made = make(r3)
    builder1._collect(builder2._collect(made))
  }

  def Yield[T, K <: Expr, R3 <: Expr]
  (r3: T)(implicit make: MakeKeyRingPair[T, K, R3]) = {
    val made = make(r3)
    builder1._collect(builder2._collect(SngExpr(made.k, made.r)))
  }
}

trait MakeExpr[X,E <: Expr] extends (X => E)
trait HMakeExpr[X,E] extends (X => E)

object MakeExpr extends Priority2MakeExprImplicits {
  def instance[X,E <: Expr](f: X => E): MakeExpr[X,E] = new MakeExpr[X,E] {
    def apply(v1: X): E = f(v1)
  }
}

object HMakeExpr {

  implicit def hnilMakeExpr: HMakeExpr[HNil, HNil] = new HMakeExpr[HNil,HNil] {
    def apply(v1: HNil): HNil = v1
  }

  implicit def hconsMakeExpr[H, HE <: Expr, T <: HList, TE <: HList](implicit makeH: MakeExpr[H,HE], makeT: HMakeExpr[T,TE]):
  HMakeExpr[H::T,HE::TE] = new HMakeExpr[H::T,HE::TE] {
    def apply(v1: H::T) = v1 match {
      case h :: t => makeH(h) :: makeT(t)
    }
  }

}

trait Priority1MakeExprImplicits {

  implicit def numericMakeRing[N : Numeric]: MakeExpr[N,NumericExpr[N]] = MakeExpr.instance { v1: N => NumericExpr(v1) }

  implicit def productMakeExpr[P <: Product,PRepr <: HList, KRepr <: HList]
  (implicit gen: Generic.Aux[P,PRepr],
   make: HMakeExpr[PRepr,KRepr],
   lub: LUBConstraint[KRepr,Expr]): MakeExpr[P,ProductExpr[KRepr]] =
    new MakeExpr[P,ProductExpr[KRepr]] {
      def apply(v1: P): ProductExpr[KRepr] = ProductExpr(make(gen.to(v1)))
    }
}


trait Priority2MakeExprImplicits extends Priority1MakeExprImplicits {
  implicit def idMakeExpr[E <: Expr]: MakeExpr[E,E] = new MakeExpr[E,E] { def apply(v1: E) = v1 }
}

trait MakeKeyRingPair[X,K <: Expr,R <: Expr] extends (X => KeyRingPair[K,R])

object MakeKeyRingPair {

  implicit def IdMakeKeyRingPair[K <: Expr, R <: Expr]: MakeKeyRingPair[KeyRingPair[K,R],K,R] =
    new MakeKeyRingPair[KeyRingPair[K,R],K,R] { def apply(v1: KeyRingPair[K,R]): KeyRingPair[K,R] = v1 }

  implicit def ImplicitOne[X, K <: Expr](implicit make: MakeExpr[X,K]): MakeKeyRingPair[X,K,NumericExpr[Int]] =
    new MakeKeyRingPair[X,K,NumericExpr[Int]] {
      def apply(v1: X): KeyRingPair[K,NumericExpr[Int]] = KeyRingPair(make(v1),NumericExpr(1))
    }
}

trait Syntax {

  implicit def toExprOps[X, E <: Expr](x: X)(implicit make: MakeExpr[X, E]): ExprOps[E] = ExprOps(make(x))

  def Sum[R <: Expr](r: R): SumExpr[R] = SumExpr(r)

  def Sng[K <: Expr, R <: Expr](k: K, r: R): SngExpr[K, R] = SngExpr(k, r)

  def Sng[T, K <: Expr](t: T)(implicit make: MakeExpr[T, K]): SngExpr[K, NumericExpr[Int]] =
    SngExpr(make(t), NumericExpr(1))

  def Group[T, R <: Expr](t: T)(implicit make: MakeExpr[T,R]): GroupExpr[R] = GroupExpr(make(t))

  object For {
    def apply[V <: Expr, R <: Expr, P <: Expr](vrp: VariableRingPredicate[V,R,P]):
      ForComprehensionBuilder[V, R, P] = ForComprehensionBuilder(vrp)

    def apply[
      V1 <: Expr, V2 <: Expr, R1 <: Expr, R2 <: Expr, P1 <: Expr, P2 <: Expr
    ](vrp1: VariableRingPredicate[V1,R1,P1],
      vrp2: VariableRingPredicate[V2,R2,P2]): NestedForComprehensionBuilder[V1, V2, R1, R2, P1, P2] = {
      NestedForComprehensionBuilder(ForComprehensionBuilder(vrp1), ForComprehensionBuilder(vrp2))
    }
  }

  implicit class IfImplicit[V <: Expr, R <: Expr, P <: Expr](pair: VariableRingPredicate[V,R,P]) {
    def If[P2 <: Expr](p: P2): VariableRingPredicate[V,R,P2] = VariableRingPredicate(pair.k,pair.r,p)
  }

}