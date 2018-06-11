package slender

import scala.reflect.runtime.universe._

trait ExprImplicits {

  implicit class ExprOps[E <: Expr](e: E) {
    def eval[T](implicit evaluator: Eval[E,T]): T = evaluator(e,Map.empty)

    def evalType[T : TypeTag](implicit evaluator: Eval[E,T]): Type = typeTag[T].tpe

    def resolve[T <: Expr](implicit resolver: Resolver[E,T]): T = resolver(e)

    def shred[Shredded <: Expr](implicit shredder: Shredder[E,Shredded]): Shredded = shredder(e)

    def shreddable[Shredded <: Expr](implicit canShred: Perhaps[Shredder[E,Shredded]]) = canShred.value.isDefined

    def isEvaluable[T](implicit canEval: Perhaps[Eval[E,_]]) = canEval.value.isDefined
  }

  implicit class KeyExprOps[K <: KeyExpr](k: K) {
    def ===[K1 <: KeyExpr](k1: K1) = EqualsPredicate(k, k1)
    def =!=[K1 <: KeyExpr](k1: K1) = NotExpr(EqualsPredicate(k, k1))

    def >[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ > _, ">")
    def <[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ < _, "<")

    def -->[R <: RingExpr](r: R): KeyRingPair[K,R] = KeyRingPair(k,r)
  }

  implicit class RingExprOps[R <: RingExpr](r: R) {
    def +[R1 <: RingExpr](expr1: R1) = AddExpr(r,expr1)

    def *[R1 <: RingExpr](expr1: R1) = MultiplyExpr(r,expr1)

    def dot[R1 <: RingExpr](expr1: R1) = DotExpr(r,expr1)

    def sum = SumExpr(r)

    def unary_- = NegateExpr(r)

    def unary_! = NotExpr(r)

    def &&[R1 <: RingExpr](expr1: R1) = this.*[R1](expr1)

    def ||[R1 <: RingExpr](expr1: R1) = this.+[R1](expr1)
  }

  implicit class VariableExprOps[V <: VariableExpr[V]](v: V) {
    def <--[R <: RingExpr](r: R): VariableRingPredicate[V,R,NumericExpr[Int]] = VariableRingPredicate(v,r)
    def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[V,R] = InfiniteMappingExpr(v,r)
  }

  case class VariableRingPredicate[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](k: V, r: R, p: P = NumericExpr(1))

  case class KeyRingPair[V <: KeyExpr, R <: RingExpr](k: V, r: R)
}
