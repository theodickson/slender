package slender

import org.apache.spark.SparkContext
import shapeless.{Generic, HList, HNil, LUBConstraint, Nat, ::}


trait Expr {

  //def children: List[Expr]

//  def id = hashCode.abs.toString.take(3).toInt
//
//  def isResolved: Boolean = children.forall(_.isResolved)
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
  //override def toString = s"⟨${children.mkString(",")}⟩"
}

trait Product2Expr[C1 <: Expr, C2 <: Expr] extends Expr with ProductExpr {
  def c1: C1
  def c2: C2
}

trait Product3Expr[C1 <: Expr, C2 <: Expr, C3 <: Expr] extends Expr with ProductExpr {
  def c1: C1
  def c2: C2
  def c3: C3
}

//trait Project1Expr extends UnaryExpr {
//  def c1: Expr with C1Expr
//  override def toString = s"$c1._1"
//}
//
//trait Project2Expr extends UnaryExpr {
//  def c1: Expr with C2Expr
//  override def toString = s"$c1._2"
//}
//
//trait Project3Expr extends UnaryExpr {
//  def c1: Expr with C3Expr
//  override def toString = s"$c1._3"
//}

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
                                         (implicit val lub: LUBConstraint[Exprs, KeyExpr]) extends KeyExpr with ProductExpr {
  //def children = exprs.toList
}

//case class ProjectKeyExpr[K <: KeyExpr, N <: Nat](c1: K)(n: N) extends UnaryKeyExpr
//
//
//case class Tuple2KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2) extends BinaryKeyExpr with Product2Expr[K1,K2]
//
//case class Tuple3KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr, K3 <: KeyExpr](c1: K1, c2: K2, c3: K3)
//  extends TernaryKeyExpr with Product3Expr[K1,K2,K3]

//case class Project1KeyExpr[K <: KeyExpr with C1Expr](c1: K) extends UnaryKeyExpr with Project1Expr
//
//case class Project2KeyExpr[K <: KeyExpr with C2Expr](c1: K) extends UnaryKeyExpr with Project2Expr
//
//case class Project3KeyExpr[K <: KeyExpr with C3Expr](c1: K) extends UnaryKeyExpr with Project3Expr

case class BoxedRingExpr[R <: Expr](c1: R) extends UnaryKeyExpr {
  override def toString = s"[$c1]"
}

case class LabelExpr[R <: RingExpr](c1: R) extends UnaryKeyExpr {
  //override def toString = s"Label($id)"
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
  extends PrimitiveRingExpr[C[K,R]] {
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

case class JoinExpr[E1 <: RingExpr, E2 <: RingExpr](c1: E1, c2: E2) extends BinaryRingOpExpr {
  def opString = "⨝"
}

case class NotExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

case class NegateExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

case class SumExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

case class GroupExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

/**Mapping constructs*/
case class InfiniteMappingExpr[K <: VariableExpr,R <: RingExpr](key: K, value: R)
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

case class ProductRingExpr[Exprs <: HList](exprs: Exprs)
                                          (implicit val lub: LUBConstraint[Exprs, RingExpr]) extends RingExpr with ProductExpr {
  //def children = exprs.toList
}

//case class Tuple2RingExpr[K1 <: RingExpr, K2 <: RingExpr](c1: K1, c2: K2) extends BinaryRingExpr with Product2Expr[K1,K2]
//
//case class Tuple3RingExpr[K1 <: RingExpr, K2 <: RingExpr, K3 <: RingExpr](c1: K1, c2: K2, c3: K3)
//  extends TernaryRingExpr with Product3Expr[K1,K2,K3]

//case class Project1RingExpr[K <: RingExpr with C1Expr](c1: K) extends UnaryRingExpr with Project1Expr
//
//case class Project2RingExpr[K <: RingExpr with C2Expr](c1: K) extends UnaryRingExpr with Project2Expr
//
//case class Project3RingExpr[K <: RingExpr with C3Expr](c1: K) extends UnaryRingExpr with Project3Expr

/**VariableExpr*/
trait VariableExpr extends KeyExpr {
  //type Type
  //def bind(t: Type): BoundVars
}


case class UnusedVariable() extends VariableExpr with NullaryKeyExpr {
  def name = "_"
}

case class TypedVariable[T](name: String) extends VariableExpr with NullaryKeyExpr {
  //type Type = T
  override def toString = name
  //def bind(t: T) = Map(this.name -> t)
  //override def isResolved = true
}


trait UntypedVariable extends VariableExpr with NullaryKeyExpr {
  //type Type = Untyped
  def name: String
  def tag[KT]: TypedVariable[KT] = TypedVariable[KT](name)
  //def bind(t: Untyped) = ???
  override def toString = s"""$name:?""""
  //override def isResolved = false
}

//case class ProductVariableExpr[Exprs <: HList, Types <: HList](exprs: Exprs)
//                                              (implicit val trav: ToTraversable.Aux[Exprs, List, VariableExpr],
//                                               aux: ProductVariableAux[Exprs, Types]) extends VariableExpr with ProductExpr {
//  type Type = Types
//  //hard to do this because of the whole types thing, in fact they have to match the eval type to tuples.
//  def children = exprs.toList
//  def bind(t: Type): BoundVars = exprs.toList.foldRight(Map.empty[String,Any])((v,acc) => acc ++ v.bind(t))
//}
//
//trait ProductVariableAux[Exprs <: HList, Type]
//
//object ProductVariableAux {
//  implicit def HNilAux: ProductVariableAux[HNil, HNil] = new ProductVariableAux[HNil, HNil] {}
//  implicit def HConsAux[H <: VariableExpr, T <: HList, Type]
//  (implicit aux: ProductVariableAux[T, Type]): ProductVariableAux[H :: T, H#Type] = new ProductVariableAux[H :: T, H#Type] {}
//}


//sealed trait ProductVariableExpr[Exprs <: HList] extends VariableExpr with ProductExpr {
//  def hChildren: Exprs
//}
//
//case object ProductVariableExprNil extends ProductVariableExpr[HNil] {
//  type Type = HNil
//  def children = List.empty[VariableExpr]
//  def hChildren = HNil
//  def bind(t: HNil): BoundVars = Map.empty
//}
//
//case class ProductVariableExprCons[H <: VariableExpr, T <: HList]
//  (head: H, tail: ProductVariableExpr[T])(implicit val trav: ToTraversable.Aux[H :: T, List, Expr]) extends ProductVariableExpr[H :: T] {
//  type Type = head.Type :: tail.Type
//  def hChildren: H :: T = head :: tail.hChildren
//  def children = hChildren.toList
//  def bind(t: Type): BoundVars = head.bind(t.head) ++ tail.bind(t.tail)
//}

//object ProductVariableExpr {
////  def apply[H <: VariableExpr, T <: HList](r: H :: T)
////                          (implicit trav: ToTraversable.Aux[H :: T, List, VariableExpr]): ProductVariableExpr[H :: T] = r match {
////    //todo - dirty hacks
////    case h :: HNil => ProductVariableExprCons[H,T](h, ProductVariableExprNil.asInstanceOf[ProductVariableExpr[T]])
////    case h :: (h1 :: t1) => ProductVariableExprCons(h, ProductVariableExpr(h1 :: t1).asInstanceOf[ProductVariableExpr[T]])
////  }
//  def apply[T <: HList](r: T)(implicit trav: ToTraversable.Aux[T, List, VariableExpr]): ProductVariableExpr[T] = r match {
//    //todo - dirty hacks
//    case HNil => ProductVariableExprNil.asInstanceOf[ProductVariableExpr[T]]
//    case h :: t => ProductVariableExprCons(h, ProductVariableExpr(t)).asInstanceOf[ProductVariableExpr[T]]
//    //case h :: (h1 :: t1) => ProductVariableExprCons(h, ProductVariableExpr(h1 :: t1)).asInstanceOf[ProductVariableExpr[T]]
//  }
//}

case class ProductVariableExpr[Exprs <: HList](exprs: Exprs)
                                              (implicit val lub: LUBConstraint[Exprs, VariableExpr]) extends VariableExpr with ProductExpr {
  //def children = exprs.toList
}
//
//case class Tuple2VariableExpr[V1 <: VariableExpr,V2 <: VariableExpr](c1: V1, c2: V2)
//  extends VariableExpr with BinaryExpr with Product2Expr[V1,V2] {
//  //type Type = (c1.Type,c2.Type)
//  //def bind(t: (c1.Type,c2.Type)) = c1.bind(t._1) ++ c2.bind(t._2)
//}
//
//case class Tuple3VariableExpr[V1 <: VariableExpr,V2 <: VariableExpr,V3 <: VariableExpr](c1: V1, c2: V2, c3: V3)
//  extends VariableExpr with BinaryExpr with Product3Expr[V1,V2,V3] {
//  //type Type = (c1.Type,c2.Type,c3.Type)
//  //def bind(t: (c1.Type,c2.Type,c3.Type)) = c1.bind(t._1) ++ c2.bind(t._2) ++ c3.bind(t._3)
//}