package slender

import org.apache.spark.SparkContext
import shapeless.{Generic, HList, HNil, LUBConstraint, Nat, ::}


trait Expr

trait C1Expr extends Expr { def c1: Expr }
trait C2Expr extends Expr { def c2: Expr }
trait C3Expr extends Expr { def c3: Expr }

trait NullaryExpr extends Expr {
  //type Children = HNil
  //def children = List.empty[Expr]
}

trait UnaryExpr extends Expr with C1Expr {
  def children = List(c1)
}

trait BinaryExpr extends Expr with C1Expr with C2Expr {
  def children = List(c1, c2)
}

trait TernaryExpr extends Expr with C1Expr with C2Expr with C3Expr {
  def children = List(c1, c2, c3)
}


trait PrimitiveExpr[T] extends NullaryExpr {
  def value: T
  override def toString = value.toString
}

case class PrimitiveKeyExpr[T](value: T) extends PrimitiveExpr[T]

object IntExpr {
  def apply(i: Int) = PrimitiveKeyExpr(i)
}

object StringExpr {
  def apply(s: String) = PrimitiveKeyExpr(s)
}

case class NumericExpr[N : Numeric](value: N) extends PrimitiveExpr[N]

case class PhysicalCollection[C[_,_],K,R](value: C[K,R])(implicit collection: Collection[C,K,R])
  extends PrimitiveExpr[C[K,R]] {
  def distribute(implicit sc: SparkContext, coll: Collection[PairRDD,K,R]): PhysicalCollection[PairRDD,K,R] = {
    val rdd: PairRDD[K,R] = value match {
      case p : PairRDD[K,R] => p
      case m : Map[K,R] => sc.parallelize(m.toSeq)
    }
    PhysicalCollection(rdd)
  }
}

object PhysicalCollection {
  def apply[T](value: Set[T])(implicit collection: Collection[Map,T,Int]): PhysicalCollection[Map,T,Int] =
    PhysicalCollection(value.map((_,1)).toMap)
}


case class ProductExpr[Exprs <: HList](exprs: Exprs)
                                      (implicit val lub: LUBConstraint[Exprs, Expr]) extends Expr {
  //def children = exprs.toList
}


/**Standard ring operations*/
trait BinaryRingOpExpr extends BinaryExpr {
  def opString: String
  //  final def brackets = ("(",")")
  //  override def toString = s"${c1.closedString} $opString ${c2.closedString}"
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
trait Predicate extends BinaryExpr {
  def opString: String
  //  override def brackets = ("(",")")
  override def toString = s"$c1 $opString $c2"
}

case class EqualsPredicate[K1 <: Expr, K2 <: Expr](c1: K1, c2: K2) extends Predicate {
  def opString = "="
}

case class IntPredicate[K1 <: Expr, K2 <: Expr](c1: K1, c2: K2, p: (Int,Int) => Boolean, opString: String)
  extends Predicate

object Predicate {
  def apply[K1 <: Expr, K2 <: Expr](k1: K1, k2: K2) = EqualsPredicate(k1,k2)
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