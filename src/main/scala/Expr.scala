package slender

import shapeless.{HList, LUBConstraint}
import shapeless.ops.hlist.ToTraversable
import shapeless.Nat
import scala.reflect.runtime.universe._

trait Expr {

  def children: List[Expr]

  def id = hashCode.abs.toString.take(3).toInt

  def isResolved: Boolean = children.forall(_.isResolved)
//  def variables: Set[Variable[_]] =
//    children.foldRight(Set.empty[Variable[_]])((v, acc) => acc ++ v.variables)
//  def freeVariables: Set[Variable[_]] =
//    children.foldRight(Set.empty[Variable[_]])((v, acc) => acc ++ v.freeVariables)
//  def labels: List[LabelExpr[_]] =
//    children.foldLeft(List.empty[LabelExpr[_]])((acc,v) => acc ++ v.labels)
//  def labelDefinitions: Seq[String] = labels.map(_.definition)

//  def brackets: (String,String) = ("","")
//  def openString = toString
//  def closedString = s"${brackets._1}$openString${brackets._2}"

//  def explainVariables: String = "\t" + variables.map(_.explain).mkString("\n\t")
//  def explainLabels: String = labelDefinitions.foldRight("")((v, acc) => acc + "\n" + v)
//  def explain: String = {
//    val border = "-"*80
//    var s = new StringBuilder(s"$border\n")
//    s ++= s"+++ $this +++"
//    s ++= s"\n\nType: $exprType"
//    s ++= s"\n\nVariable types:\n$explainVariables\n"
//    if (isShredded) s ++= s"$explainLabels\n"
//    s ++= border
//    s.mkString
//  }
}

trait C1Expr extends Expr { def c1: Expr }
trait C2Expr extends Expr { def c2: Expr }
trait C3Expr extends Expr { def c3: Expr }

trait NullaryExpr extends Expr {
  def children = List.empty[Expr]
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

trait ProductExpr extends Expr {
  override def toString = s"⟨${children.mkString(",")}⟩"
}

trait Project1Expr extends UnaryExpr {
  def c1: Expr with C1Expr
  override def toString = s"$c1._1"
}

trait Project2Expr extends UnaryExpr {
  def c1: Expr with C2Expr
  override def toString = s"$c1._2"
}

trait Project3Expr extends UnaryExpr {
  def c1: Expr with C3Expr
  override def toString = s"$c1._3"
}

trait PrimitiveExpr[V] extends NullaryExpr {
  def value: V
  override def toString = value.toString
}




/**KeyExpr*/
trait KeyExpr extends Expr

trait NullaryKeyExpr extends KeyExpr with NullaryExpr
trait UnaryKeyExpr extends KeyExpr with UnaryExpr
trait BinaryKeyExpr extends KeyExpr with BinaryExpr
trait TernaryKeyExpr extends KeyExpr with TernaryExpr

case class PrimitiveKeyExpr[T](value: T) extends KeyExpr with PrimitiveExpr[T]

object IntKeyExpr {
  def apply(i: Int) = PrimitiveKeyExpr(i)
}
object StringKeyExpr {
  def apply(s: String) = PrimitiveKeyExpr(s)
}

case class ProductKeyExpr[Exprs <: HList](exprs: Exprs)
                                          (implicit val trav: ToTraversable.Aux[Exprs, List, Expr]) extends Expr {
  def children = exprs.toList
}

case class ProjectKeyExpr[K <: KeyExpr, N <: Nat](c1: K)(n: N) extends UnaryKeyExpr

//case class HProject1KeyExpr[H <: Expr, T <: HList](c1: ProductKeyExpr[H :: T])
//                                                  (implicit lub: LUBConstraint[T, Expr])
//
//case class HProject2KeyExpr[H1 <: Expr, H2 <: Expr, T <: HList](c1: ProductKeyExpr[H1 :: H2 :: T])
//                                                               (implicit lub: LUBConstraint[T, Expr])

case class Tuple2KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2) extends BinaryKeyExpr with ProductExpr

case class Tuple3KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr, K3 <: KeyExpr](c1: K1, c2: K2, c3: K3)
  extends TernaryKeyExpr with ProductExpr

case class Project1KeyExpr[K <: KeyExpr with C1Expr](c1: K) extends UnaryKeyExpr with Project1Expr

case class Project2KeyExpr[K <: KeyExpr with C2Expr](c1: K) extends UnaryKeyExpr with Project2Expr

case class Project3KeyExpr[K <: KeyExpr with C3Expr](c1: K) extends UnaryKeyExpr with Project3Expr

case class BoxedRingExpr[R <: Expr](c1: R) extends UnaryKeyExpr {
  override def toString = s"[$c1]"
}

case class LabelExpr[R <: RingExpr](c1: R) extends UnaryKeyExpr {
  override def toString = s"Label($id)"
  //  def explainFreeVariables = freeVariables.map(_.explain).mkString("\n\t")
  //
  //  def definition: String = {
  //    val s = new StringBuilder(s"$this: ${c1.exprType}")
  //    s ++= s"\n\n\t$c1"
  //    s ++= s"\n\n\tWhere:\n\t$explainFreeVariables\n"
  //    s.mkString
  //  }
  //  override def labels = c1.labels :+ this
  //
  //  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
  //    LabelExpr(c1.replaceTypes(vars, overwrite))

  //  def renest = BoxedRingExpr(FromLabel(LabelExpr(c1.renest))) //todo
}





/**RingExpr*/
trait RingExpr extends Expr

trait NullaryRingExpr extends RingExpr with NullaryExpr
trait UnaryRingExpr extends RingExpr with UnaryExpr
trait BinaryRingExpr extends RingExpr with BinaryExpr
trait TernaryRingExpr extends RingExpr with TernaryExpr

/**Primitive ring expressions*/
trait PrimitiveRingExpr[T] extends RingExpr with PrimitiveExpr[T]

case class NumericExpr[N : Numeric](value: N) extends PrimitiveRingExpr[N]

case class PhysicalCollection[C[_,_],K,R](value: C[K,R])(implicit collection: Collection[C,K,R])
  extends PrimitiveRingExpr[C[K,R]]

object PhysicalCollection {
  def apply[T](value: Set[T])(implicit collection: Collection[Map,T,Int]): PhysicalCollection[Map,T,Int] =
    PhysicalCollection(value.map((_,1)).toMap)
}

/**Standard ring operations*/
trait BinaryRingOpExpr extends BinaryRingExpr {
  def opString: String
  //  final def brackets = ("(",")")
  //  override def toString = s"${c1.closedString} $opString ${c2.closedString}"
}

case class AddExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {
  def opString = "+"
}

case class MultiplyExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {
  def opString = "*"
}

case class DotExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {
  def opString = "⊙"
}

case class NotExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

case class NegateExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

case class SumExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

/**Mapping constructs*/
case class InfiniteMappingExpr[K <: VariableExpr[K],R <: RingExpr](key: K, value: R)
  extends BinaryRingExpr {
  def c1 = key; def c2 = value
  override def toString = s"{$key => $value}"
}

case class SngExpr[K <: KeyExpr,R <: RingExpr](key: K, value: R)
  extends BinaryRingExpr {
  def c1 = key; def c2 = value
}

/**Predicates*/
trait Predicate extends BinaryRingExpr {
  def opString: String
  //  override def brackets = ("(",")")
  override def toString = s"$c1 $opString $c2"
}

case class EqualsPredicate[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2) extends Predicate {
  def opString = "="
}

case class IntPredicate[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2, p: (Int,Int) => Boolean, opString: String)
  extends Predicate

object Predicate {
  def apply[K1 <: KeyExpr, K2 <: KeyExpr](k1: K1, k2: K2) = EqualsPredicate(k1,k2)
}

/**Conversions from keys*/
////trait FromK extends RingExpr with UnaryExpr[E[KeyExpr]
////
////object FromK {
////  def apply(k: KeyExpr): FromK = k.exprType match {
////    case BoxedRingType(_) => FromBoxedRing(k)
////    case LabelType(_) => FromLabel(k)
////    case t => throw InvalidFromKException(s"Cannot create ring expr from key expr with type $t")
////  }
////}
////
case class ToRingExpr[E <: Expr](c1: E) extends UnaryRingExpr
//case class FromBoxedRing[R,E <: Expr[R,E]](c1: BoxedRingExpr[R,E]) extends Expr[R,FromBoxedRing[R,E]] with UnaryExpr[R,BoxedRingExpr[R,E]] {
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    FromBoxedRing(c1.replaceTypes(vars, overwrite))
////
//  def _eval(vars: BoundVars): R = c1._eval(vars)
//}
//
//
//case class FromLabel[O,E <: Expr[O,E]](c1: LabelExpr[O,E]) extends Expr[O,FromLabel[O,E]] with UnaryExpr[Label[O,E],LabelExpr[O,E]] {
////
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = FromLabel(c1.replaceTypes(vars, overwrite))
//
//  def _eval(vars: BoundVars): O = c1._eval(vars).eval //todo - do I actually need to use the free variables stored in the label?
//  def shred = ???
////  def renest = FromK(c1.renest)
//}


/**Tupling and projection*/
//case class ProductRingExpr[L <: HList, Lub <: LUBConstraint[L, RingExpr]]
//  (l: L)(implicit lub: Lub, trav: ToTraversable.Aux[L, List, Lub] { type Out <: List[RingExpr] }) extends RingExpr with ProductExpr {
//  type Self = ProductRingExpr[L, Lub]
//  def children: List[RingExpr] = l.toList[Lub](trav)
//}

//case class ProductRingExpr[L <: HList](l: L)
//                                      (implicit lub: LUBConstraint[L,RingExpr],
//                                                toList: ToList[L, RingExpr])
//  extends RingExpr with ProductExpr {
//  type Self = ProductRingExpr[L]
//  def children: List[RingExpr] = l.toList//.asInstanceOf[List[RingExpr]]//l.toList(trav).map(_.asInstanceOf[RingExpr])//l.toList[Lub](trav)
//}

case class Tuple2RingExpr[K1 <: RingExpr, K2 <: RingExpr](c1: K1, c2: K2) extends BinaryRingExpr with ProductExpr

case class Tuple3RingExpr[K1 <: RingExpr, K2 <: RingExpr, K3 <: RingExpr](c1: K1, c2: K2, c3: K3)
  extends TernaryRingExpr with ProductExpr

case class Project1RingExpr[K <: RingExpr with C1Expr](c1: K) extends UnaryRingExpr with Project1Expr

case class Project2RingExpr[K <: RingExpr with C2Expr](c1: K) extends UnaryRingExpr with Project2Expr

case class Project3RingExpr[K <: RingExpr with C3Expr](c1: K) extends UnaryRingExpr with Project3Expr

trait VariableExpr[V <: VariableExpr[V]] extends KeyExpr {
  type Type
  def bind(t: Type): BoundVars
}

case class TypedVariable[T](name: String) extends VariableExpr[TypedVariable[T]] with NullaryKeyExpr {
  type Type = T
  override def toString = name
  def bind(t: T) = Map(this.name -> t)
  override def isResolved = true
}


trait UntypedVariable[T <: VariableExpr[T]] extends VariableExpr[T] with NullaryKeyExpr {
  type Type = Untyped
  def name: String
  def tag[KT]: TypedVariable[KT] = TypedVariable[KT](name)
  def bind(t: Untyped) = ???
  override def toString = s""""$this:?""""
  override def isResolved = false
}

case class Tuple2VariableExpr[V1 <: VariableExpr[V1],V2 <: VariableExpr[V2]](c1: V1, c2: V2)
  extends VariableExpr[Tuple2VariableExpr[V1,V2]] with BinaryExpr with ProductExpr {
  type Type = (c1.Type,c2.Type)
  def bind(t: (c1.Type,c2.Type)) = c1.bind(t._1) ++ c2.bind(t._2)
}

case class Tuple3VariableExpr[V1 <: VariableExpr[V1],V2 <: VariableExpr[V2],V3 <: VariableExpr[V3]](c1: V1, c2: V2, c3: V3)
  extends VariableExpr[Tuple3VariableExpr[V1,V2,V3]] with BinaryExpr with ProductExpr {
  type Type = (c1.Type,c2.Type,c3.Type)
  def bind(t: (c1.Type,c2.Type,c3.Type)) = c1.bind(t._1) ++ c2.bind(t._2) ++ c3.bind(t._3)
}