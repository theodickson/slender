package slender

import shapeless.{HList, LUBConstraint}

trait Expr

trait NullaryExpr extends Expr

trait UnaryExpr extends Expr

trait BinaryExpr extends Expr

trait TernaryExpr extends Expr

case class LiteralExpr[T](value: T) extends NullaryExpr {
  override def toString = value.toString
}

case class ProductExpr[Exprs <: HList](exprs: Exprs)
                                      (implicit val lub: LUBConstraint[Exprs, Expr]) extends Expr

/**Standard ring operations*/
trait BinaryRingOpExpr extends BinaryExpr {
  def opString: String
}

case class AddExpr[E1 <: Expr,E2 <: Expr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {
  def opString = "+"
}

case class MultiplyExpr[E1 <: Expr,E2 <: Expr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {
  def opString = "*"
}

case class DotExpr[E1 <: Expr,E2 <: Expr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {
  def opString = "⊙"
}

case class JoinExpr[E1 <: Expr, E2 <: Expr](c1: E1, c2: E2) extends BinaryRingOpExpr {
  def opString = "⨝"
}

case class NotExpr[E <: Expr](c1: E) extends UnaryExpr

case class NegateExpr[E <: Expr](c1: E) extends UnaryExpr

case class SumExpr[E <: Expr](c1: E) extends UnaryExpr

case class GroupExpr[E <: Expr](c1: E) extends UnaryExpr

/**Mapping constructs*/
case class InfiniteMappingExpr[K <: Expr,R <: Expr](key: K, value: R)
  extends BinaryExpr {
  def c1 = key; def c2 = value
  override def toString = s"{$key => $value}"
}

case class SngExpr[K <: Expr,R <: Expr](key: K, value: R)
  extends BinaryExpr {
  def c1 = key; def c2 = value
}

/**Predicates*/
case class Predicate[K1 <: Expr, K2 <: Expr,T1,T2](c1: K1, c2: K2, p: (T1,T2) => Boolean, opString: String)
  extends BinaryExpr

object EqualsPredicate {
  val anyEq = (x:Any,y:Any) => x == y
  def apply[K1 <: Expr, K2 <: Expr](k1: K1, k2: K2) = Predicate[K1,K2,Any,Any](k1,k2,anyEq, "==")
}

/**VariableExpr*/
case class UnusedVariable() extends NullaryExpr {
  def name = "_"
}

case class TypedVariable[T](name: String) extends NullaryExpr {
  override def toString = name
}

trait UntypedVariable extends NullaryExpr {
  def name: String
  def tag[KT]: TypedVariable[KT] = TypedVariable[KT](name)
  override def toString = s"""$name:?""""
}