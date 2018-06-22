package slender

import shapeless.{::, HList, HNil}

trait Expr[E]

trait ExprNode

case class LiteralExpr[V](value: V) extends ExprNode

case class AddExpr[E1,E2](c1: E1, c2: E2) extends ExprNode

case class MultiplyExpr[E1,E2](c1: E1, c2: E2) extends ExprNode

case class DotExpr[E1,E2](c1: E1, c2: E2) extends ExprNode

case class JoinExpr[E1, E2](c1: E1, c2: E2) extends ExprNode

case class NotExpr[E](c1: E) extends ExprNode

case class NegateExpr[E](c1: E) extends ExprNode

case class SumExpr[E](c1: E) extends ExprNode

case class GroupExpr[E](c1: E) extends ExprNode

/**Mapping constructs*/
case class InfiniteMappingExpr[K,R](key: K, value: R) extends ExprNode

case class SngExpr[K,R](key: K, value: R) extends ExprNode

/**Predicates*/
case class Predicate[K1, K2,T1,T2](c1: K1, c2: K2, p: (T1,T2) => Boolean, opString: String) extends ExprNode

object EqualsPredicate {
  val anyEq = (x:Any,y:Any) => x == y
  def apply[K1, K2](k1: K1, k2: K2) = Predicate[K1,K2,Any,Any](k1,k2,anyEq,"==")
}

/**VariableExpr*/
case class UnusedVariable() extends ExprNode

case class TypedVariable[T](name: String) extends ExprNode

trait UntypedVariable extends ExprNode {
  def name: String
  def tag[KT]: TypedVariable[KT] = TypedVariable[KT](name)
  override def toString = s"""$name:?""""
}

object Expr {
  def instance[E]: Expr[E] = new Expr[E] {}
  implicit def ExprBase[E <: ExprNode]: Expr[E] = instance[E]
  implicit def HListExpr[H:Expr,T <: HList](implicit tailExpr: Expr[T]): Expr[H::T] = instance[H::T]
  implicit def HNilExpr: Expr[HNil] = instance[HNil]
}