package slender

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import shapeless.syntax.SingletonOps
import shapeless.syntax.singleton._
import shapeless.{::, HList, HNil}

import scala.language.experimental.macros
import scala.reflect.ClassTag

/**Typeclass witnessing that E is an expression*/
trait Expr[E]

/**Base trait for all explicit ExprNodes*/
trait ExprNode

/**A LiteralExpr which directly contains its value. It is also identified by an ID type which is intended
  * to be the singleton type of the value, or some other uniquely identifying singleton type, e.g. a string
  * identifying the collection. This is so that any expression tree has a unique type which actually identifies
  * its output value in some way.
  */

case class LiteralExpr[V,ID](value: V, id: ID) extends ExprNode

object LiteralExpr {
  //convenient constructor for a 1, which is a default value in several methods in Syntax.scala.
  def One = LiteralExpr(1,1.narrow)
}


object Bag {
  //Convenient constructors for Bags from RDDs of tuples, with value implicitly one per row. E.g. what is read
  //from a database by default. DeepGeneric is an interface to translate native Scala tuples in these collections
  //into the HLists that we require.

  def apply[T,U,ID](value: RDD[T], id: ID)(implicit gen: DeepGeneric.Aux[T,U]): LiteralExpr[RDD[(U, Int)], ID] =
    LiteralExpr[RDD[(U,Int)],ID](value.map(t => (gen.to(t),1)), id)

  def apply[T,U:ClassTag,ID](value: DStream[T], id: ID)
                            (implicit gen: DeepGeneric.Aux[T,U]): LiteralExpr[IncDStream.Aux[(U, Int)], ID] =
    LiteralExpr[IncDStream.Aux[(U,Int)],ID](IncDStream(value.map(t => (gen.to(t),1))), id)

}

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

/**For a key expression K which evaluates to a type T, apply the T => U function to the output.*/
case class ApplyExpr[K,T,U](c1: K, f: T => U) extends ExprNode

object ApplyExpr {
  case class Factory[T,U](f: T => U) {
    def $[K](k: K): ApplyExpr[K,T,U] = ApplyExpr(k,f)
  }

  case class Factory2[T1,T2,U](f: (T1,T2) => U) {
    def $[K1,K2](k1: K1, k2: K2): ApplyExpr[K1::K2::HNil,T1::T2::HNil,U] = {
      val f0 = (x: T1::T2::HNil) => x match { case (t1::t2::HNil) => f(t1,t2) }
      ApplyExpr(k1::k2::HNil,f0)
    }
  }
}

/**VariableExpr*/
case class UnusedVariable() extends ExprNode

case class TypedVariable[T](name: String) extends ExprNode

trait Variable[Name] extends ExprNode {
  def name: Name
  def tag[KT]: TypedVariable[KT] = TypedVariable[KT](name.toString)
  override def toString = s"""$name:?""""
}

object Variable {
  //Convenient constructor for variables which automatically narrows the literal string argument to its
  //singleton type. Thus writing Variable("x") will result in a value of type Variable[String("a")].
  //Using these singleton types is necessary for inferring the types of these DSL variables at compile-time.
  implicit def apply(a: SingletonOps): Variable[a.T] = new Variable[a.T] { val name = a.narrow }
}

/**Contains implicit methods to inductively construct instances of Expr[T] for all valid expressions*/
object Expr {

  def instance[E]: Expr[E] = new Expr[E] {}

  /**Expressions are instances of an explicit ExprNode*/
  implicit def ExprNode[E <: ExprNode]: Expr[E] = instance[E]

  /**Or arbitrary products of expressions*/
  implicit def HListExpr[H:Expr,T <: HList](implicit tailExpr: Expr[T]): Expr[H::T] = instance[H::T]
  implicit def HNilExpr: Expr[HNil] = instance[HNil]
}