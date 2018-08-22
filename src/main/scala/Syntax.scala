package slender

import org.apache.spark.rdd.RDD
import shapeless._
import shapeless.syntax.SingletonOps

import scala.reflect.runtime.universe.{Type, TypeTag, WeakTypeTag, typeTag}

case class VariableRingPredicate[V:Expr, R:Expr, P:Expr](k: V, r: R, p: P = LiteralExpr.One)

case class KeyRingPair[K:Expr, R:Expr](k: K, r: R)

case class ExprOps[E:Expr](e: E) {

  def eval[R,Repr,T](implicit resolve: Resolver[E,R], evaluator: Eval[R,Repr], tupler: DeepTupler[Repr,T]): T =
    tupler(evaluator(resolve(e),Map.empty))

  def shreddedEval[T,R,C <: HList]
  (implicit shredResolve: ShreddedResolver[E,R], eval: ShreddedEval[R,T,C]): ShreddedResult[T,C] =
    eval(shredResolve(e),Map.empty)

  def -->[R:Expr](r: R): KeyRingPair[E,R] = KeyRingPair(e,r)

  def +[R1:Expr](expr1: R1) = AddExpr(e,expr1)
  def *[R1:Expr](expr1: R1) = MultiplyExpr(e,expr1)
  def dot[R1:Expr](expr1: R1) = DotExpr(e,expr1)
  def join[R1:Expr](expr1: R1) = JoinExpr(e,expr1)
  def sum = SumExpr(e)
  def unary_- = NegateExpr(e)
  def unary_! = NotExpr(e)
  def &&[R1:Expr](expr1: R1) = this.*[R1](expr1)
  def ||[R1:Expr](expr1: R1) = this.+[R1](expr1)

  def <--[R:Expr](r: R) = VariableRingPredicate(e,r)
  def ==>[R:Expr](r: R): InfiniteMappingExpr[E,R] = InfiniteMappingExpr(e,r)
}

case class ForComprehensionBuilder[V:Expr, R:Expr, P:Expr](vrp: VariableRingPredicate[V,R,P]) {
  //todo - get rid of default NumericExpr(1)

  val x = vrp.k; val r1 = vrp.r; val p = vrp.p

  def _collect[R2:Expr](r2: R2): SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, DotExpr[R2,P]]]] =
    SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, DotExpr(r2,p))))

  def Collect[T, R2]
  (r2: T)
  (implicit make: MakeExpr[T,R2], expr: Expr[R2]) = _collect(make(r2))

  def Yield[T,K,R2]
  (x: T)
  (implicit make: MakeKeyRingPair[T,K,R2], kExpr: Expr[K], rExpr: Expr[R2]) = {
    val made = make(x)
    _collect(SngExpr(made.k, made.r))
  }
}

trait MakeExpr[X,E] extends (X => E)

object MakeExpr extends Priority1MakeExprImplicits {

  def instance[X,E](f: X => E): MakeExpr[X,E] = new MakeExpr[X,E] {
    def apply(v1: X): E = f(v1)
  }

  implicit def intMakeExpr: MakeExpr[Int,LiteralExpr[Int,Int]] = instance { i => LiteralExpr(i,i) }

  implicit def hconsMakeExpr[H, HE, T <: HList, TE <: HList]
  (implicit makeH: MakeExpr[H,HE], makeT: MakeExpr[T,TE]): MakeExpr[H::T,HE::TE] = new MakeExpr[H::T,HE::TE] {
    def apply(v1: H::T) = v1 match {
      case h :: t => makeH(h) :: makeT(t)
    }
  }

  implicit def hnilMakeExpr: MakeExpr[HNil, HNil] = new MakeExpr[HNil,HNil] {
    def apply(v1: HNil): HNil = v1
  }

}

trait Priority0MakeExprImplicits {
  implicit def productMakeExpr[P<:Product,Repr,E]
  (implicit gen: Generic.Aux[P,Repr], make: MakeExpr[Repr,E]): MakeExpr[P,E] =
    MakeExpr.instance { p => make(gen.to(p)) }
}

trait Priority1MakeExprImplicits extends Priority0MakeExprImplicits {
  implicit def idMakeExpr[E:Expr]: MakeExpr[E,E] = new MakeExpr[E,E] { def apply(v1: E) = v1 }
}

trait MakeKeyRingPair[X,K,R] extends (X => KeyRingPair[K,R])

object MakeKeyRingPair {

  def instance[X,K:Expr,R:Expr](f: X => KeyRingPair[K,R]): MakeKeyRingPair[X,K,R] = new MakeKeyRingPair[X,K,R] {
    def apply(v1: X): KeyRingPair[K,R] = f(v1)
  }

  implicit def IdMakeKeyRingPair[K:Expr,R:Expr]: MakeKeyRingPair[KeyRingPair[K,R],K,R] =
    new MakeKeyRingPair[KeyRingPair[K,R],K,R] { def apply(v1: KeyRingPair[K,R]): KeyRingPair[K,R] = v1 }


  implicit def ImplicitOne[X,K](implicit make: MakeExpr[X,K], expr: Expr[K]) = instance {
    v1: X => KeyRingPair(make(v1),LiteralExpr.One)
  }
}

trait Syntax {

  val __ = UnusedVariable()

  implicit def toExprOps[X,E](x: X)(implicit make: MakeExpr[X,E], expr: Expr[E]): ExprOps[E] = ExprOps(make(x))

  def Collect[E:Expr](e: E): CollectExpr[E] = CollectExpr(e)

  def Sum[R:Expr](r: R): SumExpr[R] = SumExpr(r)

  def Sng[K:Expr, R:Expr](k: K, r: R): SngExpr[K, R] = SngExpr(k, r)

  def Sng[T,K](t: T)(implicit make: MakeExpr[T, K], expr: Expr[K]) = SngExpr(make(t), LiteralExpr.One)

  def Group[T,R](t: T)(implicit make: MakeExpr[T,R], expr: Expr[R]): GroupExpr[R] = GroupExpr(make(t))

  def Lift[T,U](f: T => U) = ApplyExpr.Factory(f)

  implicit def function1ToTransformKeyExprFactory[T,U](f: T => U): ApplyExpr.Factory[T,U] =
    ApplyExpr.Factory(f)

  implicit def function2ToTransformKeyExprFactory2[T1,T2,U](f: (T1,T2) => U): ApplyExpr.Factory2[T1,T2,U] =
    ApplyExpr.Factory2(f)

  object For {
    def apply[V:Expr, R:Expr, P:Expr](vrp: VariableRingPredicate[V,R,P]):
      ForComprehensionBuilder[V, R, P] = ForComprehensionBuilder(vrp)
  }

  implicit class IfImplicit[V:Expr, R:Expr, P:Expr](pair: VariableRingPredicate[V,R,P]) {
    def If[P2:Expr](p: P2): VariableRingPredicate[V,R,P2] = VariableRingPredicate(pair.k,pair.r,p)
  }

  def Var(a: SingletonOps): Variable[a.T] = new Variable[a.T] { val name = a.narrow }

  def Vars(a1: SingletonOps, a2: SingletonOps): (Variable[a1.T],Variable[a2.T]) =
    (new Variable[a1.T] { val name = a1.narrow },new Variable[a2.T] { val name = a2.narrow })

  def Vars(a1: SingletonOps, a2: SingletonOps, a3: SingletonOps): (Variable[a1.T],Variable[a2.T],Variable[a3.T]) =
    (new Variable[a1.T] { val name = a1.narrow },
      new Variable[a2.T] { val name = a2.narrow },
      new Variable[a3.T] { val name = a3.narrow }
    )
}