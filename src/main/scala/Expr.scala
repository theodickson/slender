package slender

import algebra._
import slender.execution.Evaluator
import slender.shredding.Shredder
//import slender.shredding.Shredder


object types {
  type BoundVars = Map[Variable[_],Any]
}

import types._

sealed trait Expr[Self <: Expr[Self]] { self : Self =>

//  def children: Seq[Expr[Nothing,Nothing]]

  def eval[T](implicit evaluator: Evaluator[Self,T]): T = evaluator(this,Map.empty)

  def shred[Shredded <: Expr[Shredded]](implicit shredder: Shredder[Self,Shredded]): Shredded = shredder(this)

  def id = hashCode.abs.toString.take(3).toInt

//  def replaceTypes(vars: Map[String,KeyType], overwrite: Boolean): Expr
//  def inferTypes(vars: Map[String,KeyType]): T = replaceTypes(vars, false)
//  def inferTypes: T = inferTypes(Map.empty)
//
//  def variables: Set[Variable[_]] =
//    children.foldRight(Set.empty[Variable[_]])((v, acc) => acc ++ v.variables)
//  def freeVariables: Set[Variable[_]] =
//    children.foldRight(Set.empty[Variable[_]])((v, acc) => acc ++ v.freeVariables)
//  def labels: Seq[LabelExpr] =
//    children.foldLeft(Seq.empty[LabelExpr])((acc,v) => acc ++ v.labels)
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

sealed trait NullaryExpr[T <: NullaryExpr[T]] extends Expr[T] { //self : T =>
//  def children = List.empty[Expr[Nothing,Nothing]]
//  def renest = this
//  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = this
}

////todo trait for each child C1[O,S] C2[O,S] etc so project1 just needs a child which is a C1, etc.
sealed trait UnaryExpr[T] {
//  implicit def ev1: E <:< Expr[T]
//  def c1: E
//  def children = List(c1)
}

//
//sealed trait BinaryExpr[T1,T2,E1,E2] {
////  implicit def ev1: E1 <:< Expr[T1,E1]
////  implicit def ev2: E2 <:< Expr[T2,E2]
////  def c1: E1
////  def c2: E2
////  def children = List(c1, c2)
//}
//
//sealed trait ProductExpr {
//  def children: Seq[Expr[_,Nothing]]
//  override def toString = s"⟨${children.mkString(",")}⟩"
//}
//
//sealed trait Tuple2Expr[T1,T2] extends Expr[(T1,T2),Tuple2Expr[T1,T2]] with BinaryExpr {
//  def c1: Expr[T1,Nothing]
//  def c2: Expr[T2,Nothing]
//}

//sealed trait Tuple3Expr
//
//
//sealed trait ProjectExpr[T <: Expr[T]] extends Expr[T] with UnaryExpr[T] {
//  def n: Int
//  override def toString = s"$c1._$n"
//}





sealed trait PrimitiveExpr[E <: PrimitiveExpr[E,V],V] extends NullaryExpr[E] {
  def value: V
}

//case class PrimitiveKeyExpr[T](value: T)
//                              //(implicit val ev1: TypeTag[T], val ev2: Liftable[T])
//  extends NullaryKeyExpr[T] with PrimitiveExpr[T] {
////  val exprType = PrimitiveKeyType(typeOf[T])
//  override def toString = value.toString
//}

//object IntKeyExpr {
//  def apply(i: Int) = PrimitiveKeyExpr(i)
//}
//
//object StringKeyExpr {
//  def apply(s: String) = PrimitiveKeyExpr(s)
//}


//case class KeyProductExpr(children: List[KeyExpr]) extends KeyExpr with ProductExpr[KeyExpr] {
//
//  val exprType =
//    if (children.map(_.exprType).forall(_.isResolved)) ProductKeyType(children.map(_.exprType))
//    else UnresolvedKeyType
//
//  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
//    KeyProductExpr(children.map(_.replaceTypes(vars, overwrite)))
//
//  def shred = KeyProductExpr(children.map(_.shred))
//  def renest = KeyProductExpr(children.map(_.renest))
//}
//
//object KeyProductExpr {
//  def apply(exprs: KeyExpr*) = new KeyProductExpr(exprs.toList)
//}
//
//
//case class ProjectKeyExpr(c1: KeyExpr, n: Int) extends KeyExpr with ProjectExpr[KeyExpr] with UnaryExpr[KeyExpr] {
//
//  val exprType = c1.exprType.project(n)
//  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
//    ProjectKeyExpr(c1.replaceTypes(vars, overwrite), n)
//  def shred = ProjectKeyExpr(c1.shred, n)
//  def renest = ProjectKeyExpr(c1.renest, n)
//  override def toString = c1 match {
//    case TypedVariable(name, kt) => s""""$name"._$n"""
//    case UntypedVariable(name) => s""""$name._$n:?"""
//    case _ => s"$c1._$n"
//  }
//}


sealed trait VariableExpr {
  //def matchTypes(keyType: KeyType): Map[String,KeyType]
//  def _eval(vars: BoundVars): O = ???
}

//sealed trait VariableName

case class Variable[O](name: String) extends Expr[Variable[O]] with VariableExpr  {

  override def toString = s""""$name""""
//  override def explain: String = s""""$name": $exprType"""
//  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = vars.get(name) match {
//    case None | Some(`exprType`) => this
//    case Some(otherType) => if (overwrite) TypedVariable(name, otherType) else
//      throw VariableResolutionConflictException(
//        s"Tried to resolve var $name with type $otherType, already had type $exprType, overwriting false."
//      )
//  }
//  override def variables = Set(this)
//  override def freeVariables = Set(this)
//  override def _eval(vars: BoundVars): O = vars.get(this) match {
//    case v: Option[O] => v.get
//    case _ => throw new IllegalStateException()
//  }
}


//case class ProductVariableKeyExpr(children0: List[KeyExpr]) extends ProductExpr[KeyExpr] with VariableKeyExpr {
//  def exprType: KeyType =
//    if (children.forall(_.isResolved)) ProductKeyType(children.map(_.exprType))
//    else UnresolvedKeyType
//  def children: List[VariableKeyExpr] = children0.map(_.asInstanceOf[VariableKeyExpr])
//  def shred = this
//  def renest = this
//  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean): KeyExpr = ProductVariableKeyExpr(
//    children.map(_.replaceTypes(vars,overwrite))
//  )
//  def matchTypes(keyType: KeyType): Map[String,KeyType] = keyType match {
//    case ProductKeyType(ts) => children.zip(ts) map { case (expr,typ) => expr.matchTypes(typ) } reduce { _ ++ _ }
//    case _ => ???
//  }
//  override def toString = s"⟨${children.mkString(",")}⟩"
//}


//sealed trait ToK[O,S] extends KeyExpr[O,S] with UnaryExpr

//case class BoxedRingExpr[O,E <: Expr[O]](c1: E)(implicit ring: Ring[O]) extends Expr[O] with UnaryExpr[O,E] {
////  val exprType = c1.exprType.box
//  override def toString = s"[$c1]"
//  def _eval(vars: BoundVars) = c1._eval(vars)
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    BoxedRingExpr(c1.replaceTypes(vars, overwrite))
////  def renest = ???
//}
//
//
//case class Label[T,E <: Expr[T]](expr: E, vars: BoundVars) {
//  def eval = expr._eval(vars)
//}
////
//case class LabelExpr[O,E <: Expr[O]](c1: E) extends Expr[Label[O,E]] with UnaryExpr[O,E] {
////  val exprType =
////    if (!c1.isResolved) throw new IllegalStateException("Cannot create labeltype from unresolved ring type.")
////    else LabelType(c1.exprType)
//
//  def _eval(vars: BoundVars): Label[O,E] = Label(c1,vars) //todo - filter?
//
//  override def toString = s"Label($id)"
//
////  def explainFreeVariables = freeVariables.map(_.explain).mkString("\n\t")
////
////  def definition: String = {
////    val s = new StringBuilder(s"$this: ${c1.exprType}")
////    s ++= s"\n\n\t$c1"
////    s ++= s"\n\n\tWhere:\n\t$explainFreeVariables\n"
////    s.mkString
////  }
////  override def labels = c1.labels :+ this
////
////  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    LabelExpr(c1.replaceTypes(vars, overwrite))
//
////  def renest = BoxedRingExpr(FromLabel(LabelExpr(c1.renest))) //todo
//
//}


case class IntExpr(value: Int) extends Expr[IntExpr] with PrimitiveExpr[IntExpr,Int]


case class PhysicalCollection[C[_,_],K,R](value: C[K,R])//(implicit ev: Collection[C,K,R])
  extends PrimitiveExpr[PhysicalCollection[C,K,R],C[K,R]]





//case class InfiniteMappingExpr[K,R,V <: Expr[R,V]](key: Variable[K], value: V)
//  extends Expr[K => R,InfiniteMappingExpr[K,R,V]] with BinaryExpr[K,R,Variable[K],V] {
//
//  def c1 = key; def c2 = value
//  def _eval(vars: BoundVars) = (k: K) => value._eval(vars + (key -> k))
//
//  override def toString = s"{$key => $value}"
//
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean): RingExpr =
////    InfiniteMappingExpr(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))
////
//
////  def renest = InfiniteMappingExpr(key.renest, value.renest)
//}


//sealed trait BinaryRingOpExpr[T1,T2,E1,E2,O] extends BinaryExpr[T1,T2,E1,E2] {
//  def opString: String
////  final def brackets = ("(",")")
////  override def toString = s"${c1.closedString} $opString ${c2.closedString}"
//}


//case class AddExpr[O,E1 <: Expr[O,E1],E2 <: Expr[O,E2]](c1: E1, c2: E2)(implicit ring: Ring[O])
//  extends Expr[O,AddExpr[O,E1,E2]] with BinaryRingOpExpr[O,O,E1,E2,O] {
////  val exprType: RingType = c1.exprType + c2.exprType
//
//  def opString = "+"
//  def _eval(vars: BoundVars) = ring.add(c1._eval(vars),c2._eval(vars))
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    Add(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))
////
////  def shred = Add(c1.shred, c2.shred)
////  def renest = Add(c1.renest, c2.renest)
//}
//
//
//case class MultiplyExpr[T1,T2,E1 <: Expr[T1,E1],E2 <: Expr[T2,E2],O]
//                       (c1: E1, c2: E2)
//                       (implicit mult: Multiply[T1,T2,O])
//  extends Expr[O,MultiplyExpr[T1,T2,E1,E2,O]] with BinaryRingOpExpr[T1,T2,E1,E2,O] {
////  val exprType: RingType = c1.exprType * c2.exprType
//  def opString = "*"
//  def _eval(vars: BoundVars) = mult(c1._eval(vars),c2._eval(vars))
////  override def freeVariables = c2 match {
////    case InfiniteMappingExpr(k: VariableKeyExpr, _) => c1.freeVariables ++ c2.freeVariables -- k.freeVariables
////    case _ => c1.freeVariables ++ c2.freeVariables
////  }
//
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = {
////    val newC1 = c1.replaceTypes(vars, overwrite)
////    val newC2 = newC1.exprType match {
////      case FiniteMappingType(keyType,_,_) => c2 match {
////        case InfiniteMappingExpr(v: VariableKeyExpr,_) => c2.replaceTypes(vars ++ v.matchTypes(keyType), overwrite)
////        case _ => c2.replaceTypes(vars, overwrite)
////      }
////      case _ => c2.replaceTypes(vars, overwrite)
////    }
////    Multiply(newC1, newC2)
////  }
////
////  def shred = {
////    val newC1 = c1.shred
////    val newC2 = newC1.exprType match {
////      case FiniteMappingType(keyType,_,_) => c2 match {
////        case InfiniteMappingExpr(v: VariableKeyExpr,_) => {
////          val replaced = c2.replaceTypes(v.matchTypes(keyType), true)
////          replaced.shred
////        }
////        case _ => c2.shred
////      }
////      case _ => c2.shred
////    }
////    Multiply(newC1, newC2)
////  }
////
////  def renest = { //todo refactor
////    val newC1 = c1.renest
////    val newC2 = newC1.exprType match {
////      case FiniteMappingType(keyType,_,_) => c2 match {
////        case InfiniteMappingExpr(v: VariableKeyExpr,_) => {
////          val replaced = c2.replaceTypes(v.matchTypes(keyType), true)
////          replaced.renest
////        }
////        case _ => c2.renest
////      }
////      case _ => c2.renest
////    }
////    Multiply(newC1, newC2)
////  }
//}


//case class DotExpr[T1,T2,E1,E2,O](c1: E1, c2: E2)(implicit dot: Dot[T1,T2,O], val ev1: E1 <:< Expr[T1], val ev2: E2 <:< Expr[T2])
//extends Expr[O] with BinaryRingOpExpr[T1,T2,E1,E2,O] {
//  type Self = DotExpr[T1,T2,E1,E2,O]
//  def opString = "⊙"
//  def _eval(vars: BoundVars) = dot(c1._eval(vars),c2._eval(vars))
//}

//case class SelfDotExpr[T1,E1,O](c1: E1, c2: E1)(dot: Dot[T1,T1,O])(implicit ev1: E1 <:< Expr[T1])
//  extends Expr[O] {
//  type Self = SelfDotExpr[T1,E1,O]
//  def opString = "⊙"
//  def _eval(vars: BoundVars) = dot(c1._eval(vars),c2._eval(vars))
//}
//
//case class SelfDotExpr2[T1,E1,O](c1: E1, c2: E1)(implicit dot: Dot[T1,T1,O], ev1: E1 <:< Expr[T1])
//  extends Expr[O] {
//  type Self = SelfDotExpr[T1,E1,O]
//  def opString = "⊙"
//  def _eval(vars: BoundVars) = dot(c1._eval(vars),c2._eval(vars))
//}

case class SelfDotExpr[E <: Expr[E]](c1: E, c2: E)
  extends Expr[SelfDotExpr[E]] {
  def opString = "⊙"
}


//trait FromExpr[T,E]
//
//implicit def

//case class DotExpr[E1 <: Expr, E2 <: Expr,O](c1: E1, c2: E2)(implicit dot: Dot[c1.T,c2.T,O])
////(implicit dot: Dot[T1,T2,O])
//  extends Expr {
//  type Self = DotExpr[E1,E2,O]
//  type T = O
//  def opString = "⊙"
//  def _eval(vars: BoundVars) = ??? // dot(c1._eval(vars),c2._eval(vars))
//}



//case class SumExpr[C[_,_],K,R,E <: Expr[C[K,R],E]](c1: E)
//                              (implicit coll: Collection[C,K,R])
//  extends Expr[R,SumExpr[C,K,R,E]] with UnaryExpr[C[K,R],E] {
//  def _eval(vars: BoundVars): R = coll.sum(c1._eval(vars))
////  override val exprType: RingType = c1.exprType.sum
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = Sum(c1.replaceTypes(vars, overwrite))
////  def shred = Sum(c1.shred)
////  def renest = Sum(c1.renest)
//}
////
////
////case class Not(c1: RingExpr) extends UnaryRingExpr {
////  val exprType = c1.exprType
////  override def toString = s"¬${c1.closedString}"
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = Not(c1.replaceTypes(vars, overwrite))
////  def shred = Not(c1.shred)
////  def renest = Not(c1.renest)
////}
////
////
////case class Negate(c1: RingExpr) extends UnaryRingExpr {
////  val exprType = c1.exprType
////  override def toString = s"-${c1.closedString}"
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    Negate(c1.replaceTypes(vars, overwrite))
////  def shred = Negate(c1.shred)
////  def renest = Negate(c1.renest)
////}
////
////sealed trait Predicate extends RingExpr with BinaryExpr[KeyExpr,KeyExpr] {
////  def opString: String
////  override def brackets = ("(",")")
////  override def toString = s"$c1 $opString $c2"
////}
////
////case class EqualsPredicate(c1: KeyExpr, c2: KeyExpr) extends Predicate {
////  val exprType = c1.exprType compareEq c2.exprType
////  def opString = "="
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    EqualsPredicate(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))
////  def shred = EqualsPredicate(c1.shred, c2.shred)
////  def renest = EqualsPredicate(c1.renest, c2.renest)
////}
////
////case class IntPredicate(c1: KeyExpr, c2: KeyExpr, p: (Int,Int) => Boolean, opString: String)
////                       (implicit val ev1: TypeTag[(Int,Int) => Boolean])
////  extends Predicate {
////  val exprType = c1.exprType compareOrd c2.exprType
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    IntPredicate(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite), p, opString)
////  def shred = IntPredicate(c1.shred, c2.shred, p, opString)
////  def renest = IntPredicate(c1.renest, c2.renest, p, opString)
////  def literal = reify(p)
////}
////
////
//case class Sng[K,R:Ring,KE <: Expr[K,KE],RE <: Expr[R,RE]](key: KE, value: RE)
//  extends Expr[Map[K,R],Sng[K,R,KE,RE]] with BinaryExpr[K,R,KE,RE] {
////  val keyType = key.exprType
////  val valueType = value.exprType
//////  assert(keyType != UnresolvedKeyType && valueType != UnresolvedRingType)
////  assert(!valueType.isInstanceOf[InfiniteMappingType])
////  val exprType = (keyType,valueType) match {
////    case (UnresolvedKeyType,_) | (_,UnresolvedRingType) => UnresolvedRingType
////    case _ => FiniteMappingType(keyType,valueType,false)
////  }
//  def c1 = key; def c2 = value
//
//  def _eval(vars: BoundVars): Map[K,R] = Map(key._eval(vars) -> value._eval(vars))
//
////  override def toString = value match {
////    case IntExpr(1) => s"sng($key)"
////    case _ => s"sng($key, $value)"
////  }
//
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    Sng(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))
////  def renest = Sng(key.renest, value.renest)
//}
//
//
////sealed trait FromK extends RingExpr with UnaryExpr[E[KeyExpr]
////
////object FromK {
////  def apply(k: KeyExpr): FromK = k.exprType match {
////    case BoxedRingType(_) => FromBoxedRing(k)
////    case LabelType(_) => FromLabel(k)
////    case t => throw InvalidFromKException(s"Cannot create ring expr from key expr with type $t")
////  }
////}
////
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



case class InvalidPredicateException(str: String) extends Exception(str)
case class InvalidFromLabelException(msg: String) extends Exception(msg)
case class VariableResolutionConflictException(msg: String) extends Exception(msg)
case class IllegalResolutionException(msg: String) extends Exception(msg)
case class IllegalFreeVariableRequestException(msg: String) extends Exception(msg)
case class InvalidShreddingException(msg: String) extends Exception(msg)
case class InvalidFromKException(str: String) extends Exception(str)