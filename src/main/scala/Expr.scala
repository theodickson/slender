package slender

import definitions._

import scala.collection.mutable.StringBuilder
import scala.reflect.runtime.universe._


sealed trait Expr[T <: Expr[T]] {

  def exprType: ExprType[_]

  def children: Seq[Expr[_]]

  def shred: T
  def renest: T

  def id = hashCode.abs.toString.take(3).toInt

  def replaceTypes(vars: Map[String,KeyType], overwrite: Boolean): T
  def inferTypes(vars: Map[String,KeyType]): T = replaceTypes(vars, false)
  def inferTypes: T = inferTypes(Map.empty)

  def variables: Set[Variable] =
    children.foldRight(Set.empty[Variable])((v, acc) => acc ++ v.variables)
  def freeVariables: Set[Variable] =
    children.foldRight(Set.empty[Variable])((v, acc) => acc ++ v.freeVariables)
  def labels: Seq[LabelExpr] =
    children.foldLeft(Seq.empty[LabelExpr])((acc,v) => acc ++ v.labels)
  def labelDefinitions: Seq[String] = labels.map(_.definition)

  def isShredded: Boolean = children.exists(_.isShredded)
  def isResolved: Boolean = exprType.isResolved
  def isComplete: Boolean = freeVariables.isEmpty

  def brackets: (String,String) = ("","")
  def openString = toString
  def closedString = s"${brackets._1}$openString${brackets._2}"

  def explainVariables: String = "\t" + variables.map(_.explain).mkString("\n\t")
  def explainLabels: String = labelDefinitions.foldRight("")((v, acc) => acc + "\n" + v)
  def explain: String = {
    val border = "-"*80
    var s = new StringBuilder(s"$border\n")
    s ++= s"+++ $this +++"
    s ++= s"\n\nType: $exprType"
    s ++= s"\n\nVariable types:\n$explainVariables\n"
    if (isShredded) s ++= s"$explainLabels\n"
    s ++= border
    s.mkString
  }
}

sealed trait NullaryExpr[T <: Expr[T]] extends Expr[T] { self : T =>
  def children = List.empty[Expr[_]]
  def shred = this
  def renest = this
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = this
}


sealed trait UnaryExpr[T <: Expr[T]] {
  def c1: Expr[T]
  def children = List(c1)
}


sealed trait BinaryExpr[T1 <: Expr[T1], T2 <: Expr[T2]] {
  def c1: Expr[T1]
  def c2: Expr[T2]
  def children = List(c1, c2)
}


sealed trait ProductExpr[T <: Expr[T]] extends Expr[T] {
  def children: List[Expr[T]]
  override def toString = s"⟨${children.mkString(",")}⟩"
}


sealed trait ProjectExpr[T <: Expr[T]] extends Expr[T] with UnaryExpr[T] {
  def n: Int
  override def toString = s"$c1._$n"
}



sealed trait KeyExpr extends Expr[KeyExpr] {
  def exprType: KeyType
}


sealed trait NullaryKeyExpr extends KeyExpr with NullaryExpr[KeyExpr]


sealed trait PrimitiveExpr[T <: Expr[T], V] extends Expr[T] {
  def value: V
  implicit def ev1: TypeTag[V]
  def literal = reify[V](value).tree
}

case class PrimitiveKeyExpr[T](value: T)
                              (implicit val ev1: TypeTag[T], val ev2: Liftable[T])
  extends NullaryKeyExpr with PrimitiveExpr[KeyExpr, T] {
  val exprType = PrimitiveKeyType(typeOf[T])
  override def toString = value.toString
}

object IntKeyExpr {
  def apply(i: Int) = PrimitiveKeyExpr(i)
}

object StringKeyExpr {
  def apply(s: String) = PrimitiveKeyExpr(s)
}


case class KeyProductExpr(children: List[KeyExpr]) extends KeyExpr with ProductExpr[KeyExpr] {
  val exprType = if (children.map(_.exprType).forall(_.isResolved))
    ProductKeyType(children.map(_.exprType)) else UnresolvedKeyType

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    KeyProductExpr(children.map(_.replaceTypes(vars, overwrite)))

  def shred = KeyProductExpr(children.map(_.shred))
  def renest = KeyProductExpr(children.map(_.renest))
}

object KeyProductExpr {
  def apply(exprs: KeyExpr*) = new KeyProductExpr(exprs.toList)
}


case class ProjectKeyExpr(c1: KeyExpr, n: Int) extends KeyExpr with ProjectExpr[KeyExpr] with UnaryExpr[KeyExpr] {
  val exprType = c1.exprType.project(n)
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    ProjectKeyExpr(c1.replaceTypes(vars, overwrite), n)
  def shred = ProjectKeyExpr(c1.shred, n)
  def renest = ProjectKeyExpr(c1.renest, n)
  override def toString = c1 match {
    case TypedVariable(name, kt) => s""""$name"._$n"""
    case UntypedVariable(name) => s""""$name._$n:?"""
    case _ => s"$c1._$n"
  }
}


sealed trait VariableKeyExpr extends KeyExpr {
  def matchTypes(keyType: KeyType): Map[String,KeyType]
}

sealed trait Variable extends VariableKeyExpr with NullaryKeyExpr {
  def name: String
  override def variables = Set(this)
  override def freeVariables = Set(this)
  def matchTypes(keyType: KeyType) = Map(name -> keyType)
}

case class TypedVariable(name: String, exprType: KeyType) extends Variable {
  override def toString = s""""$name""""
  override def explain: String = s""""$name": $exprType"""
  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = vars.get(name) match {
    case None | Some(`exprType`) => this
    case Some(otherType) => if (overwrite) TypedVariable(name, otherType) else
      throw VariableResolutionConflictException(
        s"Tried to resolve var $name with type $otherType, already had type $exprType, overwriting false."
      )
  }
}


case class UntypedVariable(name: String) extends Variable {
  val exprType = UnresolvedKeyType
  override def explain = toString
  override def toString = s""""$name": ?"""
  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = vars.get(name) match {
    case None => this
    case Some(eT) => TypedVariable(name, eT)
  }
}


case class ProductVariableKeyExpr(children0: List[KeyExpr]) extends ProductExpr[KeyExpr] with VariableKeyExpr {
  def exprType: KeyType =
    if (children.forall(_.isResolved)) ProductKeyType(children.map(_.exprType))
    else UnresolvedKeyType
  def children: List[VariableKeyExpr] = children0.map(_.asInstanceOf[VariableKeyExpr])
  def shred = this
  def renest = this
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean): KeyExpr = ProductVariableKeyExpr(
    children.map(_.replaceTypes(vars,overwrite))
  )
  def matchTypes(keyType: KeyType): Map[String,KeyType] = keyType match {
    case ProductKeyType(ts) => children.zip(ts) map { case (expr,typ) => expr.matchTypes(typ) } reduce { _ ++ _ }
    case _ => ???
  }
  override def toString = s"⟨${children.mkString(",")}⟩"
}

sealed trait ToK extends KeyExpr with UnaryExpr[RingExpr]


case class BoxedRingExpr(c1: RingExpr) extends ToK {
  val exprType = c1.exprType.box
  override def toString = s"[$c1]"
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    BoxedRingExpr(c1.replaceTypes(vars, overwrite))
  def shred = LabelExpr(c1.shred)
  def renest = ???
}


case class LabelExpr(c1: RingExpr) extends ToK {
//todo - there is something screwy about unshredding. as shredding does not introduce any abstract notion
//of a context, it just totally preserves the structure, there is something not right about recursively unshredding.
//if you dont' reconstruct the LabelExpr with the recursively unshredded argument, then you preserve its ID and wrapped type
//and could theoretically recover the definition no problem, but without any deeper labels being unshredded.

//however if you do recursively unshred the argument, then you end up with differetn label IDs and definitions to the
//originally shredded expression. is this a problem? and if so, should unshredding purely be an implementation detail?
//i.e.
  val exprType = c1.exprType match {
    case UnresolvedRingType => throw new IllegalStateException("Cannot create labeltype from unresolved ring type.")
    case rt: RingType => LabelType(rt)
  }

  override def toString = s"Label($id)"

  def explainFreeVariables = freeVariables.map(_.explain).mkString("\n\t")

  def definition: String = {
    val s = new StringBuilder(s"$this: ${c1.exprType}")
    s ++= s"\n\n\t$c1"
    s ++= s"\n\n\tWhere:\n\t$explainFreeVariables\n"
    s.mkString
  }
  override def labels = c1.labels :+ this

  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    LabelExpr(c1.replaceTypes(vars, overwrite))

  override def shred =
    throw InvalidShreddingException("Cannot shred a LabelExpr, it's already shredded.")

  def renest = BoxedRingExpr(FromLabel(LabelExpr(c1.renest))) //todo

  override def isShredded = true
}






sealed trait RingExpr extends Expr[RingExpr] {
  def exprType: RingType
  def shredAndExplain: String = {
    explain + "\n" + shred.explain + "\n\n"
  }
}


sealed trait NullaryRingExpr extends RingExpr with NullaryExpr[RingExpr]


sealed trait UnaryRingExpr extends RingExpr with UnaryExpr[RingExpr]


sealed trait MappingExpr extends RingExpr {
  def keyType: KeyType
  def valueType: RingType
  def exprType: RingType = keyType --> valueType
}


sealed trait PhysicalMapping extends MappingExpr with NullaryRingExpr {
  def ref: String
  override def toString = s"$ref"
}


sealed trait LogicalMappingExpr extends MappingExpr with BinaryExpr[KeyExpr,RingExpr] {
  def key: KeyExpr
  def value: RingExpr
  def c1 = key
  def c2 = value
}


case class IntExpr(value: Int)(implicit val ev1: TypeTag[Int]) extends NullaryRingExpr with PrimitiveExpr[RingExpr, Int] {
  val exprType = IntType
  override def toString = s"$value"
}


case class PhysicalCollection(keyType: KeyType, valueType: RingType, ref: String) extends PhysicalMapping {
  override def shred = this//ShreddedPhysicalCollection(keyType.shred, valueType.shred, ref)
}


case class PhysicalBag(keyType: KeyType, ref: String) extends PhysicalMapping {
  val valueType = IntType
  override def shred = this//ShreddedPhysicalBag(keyType.shred, ref)
}


case class ShreddedPhysicalCollection(keyType: KeyType, valueType: RingType, ref: String) extends PhysicalMapping {
  override def toString = s"${ref}_F"
  override def shred: Nothing = ???
}


case class ShreddedPhysicalBag(keyType: KeyType, ref: String) extends PhysicalMapping {
  val valueType = IntType
  override def toString = s"${ref}_F"
  override def shred: Nothing = ???
}


case class InfiniteMappingExpr(key: KeyExpr, value: RingExpr) extends LogicalMappingExpr {
  assert(key.isInstanceOf[VariableKeyExpr]) //TODO
  val keyType = key.exprType
  val valueType = value.exprType
  override val exprType = keyType ==> valueType
  override def toString = s"{$key => $value}"

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean): RingExpr =
    InfiniteMappingExpr(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))

  def shred = InfiniteMappingExpr(key.shred, value.shred)
  def renest = InfiniteMappingExpr(key.renest, value.renest)
}


case class RingProductExpr(children: List[RingExpr]) extends RingExpr with ProductExpr[RingExpr] {
  val exprType = if (children.map(_.exprType).forall(_.isResolved))
    ProductRingType(children.map(_.exprType)) else UnresolvedRingType
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    RingProductExpr(children.map(_.replaceTypes(vars, overwrite)))
  def shred = RingProductExpr(children.map(_.shred))
  def renest = RingProductExpr(children.map(_.renest))
}

object RingProductExpr {
  def apply(exprs: RingExpr*) = new RingProductExpr(exprs.toList)
}


case class ProjectRingExpr(c1: RingExpr, n: Int) extends RingExpr with ProjectExpr[RingExpr] with UnaryExpr[RingExpr] {
  val exprType = c1.exprType.project(n)
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    ProjectRingExpr(c1.replaceTypes(vars, overwrite), n)
  def shred = ProjectRingExpr(c1.shred, n)
  def renest = ProjectRingExpr(c1.renest, n)
}


sealed trait BinaryRingOpExpr extends RingExpr with BinaryExpr[RingExpr,RingExpr] {
  def opString: String
  override def brackets = ("(",")")
  override def toString = s"${c1.closedString} $opString ${c2.closedString}"
}


case class Add(c1: RingExpr, c2: RingExpr) extends BinaryRingOpExpr {
  val exprType: RingType = c1.exprType + c2.exprType

  def opString = "+"

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    Add(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))

  def shred = Add(c1.shred, c2.shred)
  def renest = Add(c1.renest, c2.renest)
}


case class Multiply(c1: RingExpr, c2: RingExpr) extends BinaryRingOpExpr {
  val exprType: RingType = c1.exprType * c2.exprType
  def opString = "*"

  override def freeVariables = c2 match {
    case InfiniteMappingExpr(k: VariableKeyExpr, _) => c1.freeVariables ++ c2.freeVariables -- k.freeVariables
    case _ => c1.freeVariables ++ c2.freeVariables
  }

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = {
    val newC1 = c1.replaceTypes(vars, overwrite)
    val newC2 = newC1.exprType match {
      case FiniteMappingType(keyType,_) => c2 match {
        case InfiniteMappingExpr(v: VariableKeyExpr,_) => c2.replaceTypes(vars ++ v.matchTypes(keyType), overwrite)
        case _ => c2.replaceTypes(vars, overwrite)
      }
      case _ => c2.replaceTypes(vars, overwrite)
    }
    Multiply(newC1, newC2)
  }

  def shred = {
    val newC1 = c1.shred
    val newC2 = newC1.exprType match {
      case FiniteMappingType(keyType,_) => c2 match {
        case InfiniteMappingExpr(v: VariableKeyExpr,_) => {
          val replaced = c2.replaceTypes(v.matchTypes(keyType), true)
          replaced.shred
        }
        case _ => c2.shred
      }
      case _ => c2.shred
    }
    Multiply(newC1, newC2)
  }

  def renest = { //todo refactor
    val newC1 = c1.renest
    val newC2 = newC1.exprType match {
      case FiniteMappingType(keyType,_) => c2 match {
        case InfiniteMappingExpr(v: VariableKeyExpr,_) => {
          val replaced = c2.replaceTypes(v.matchTypes(keyType), true)
          replaced.renest
        }
        case _ => c2.renest
      }
      case _ => c2.renest
    }
    Multiply(newC1, newC2)
  }
}


case class Dot(c1: RingExpr, c2: RingExpr) extends BinaryRingOpExpr {
  val exprType: RingType = c1.exprType dot c2.exprType
  def opString = "⊙"
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    Dot(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))
  def shred = Dot(c1.shred, c2.shred)
  def renest = Dot(c1.renest, c2.renest)
}


case class Sum(c1: RingExpr) extends UnaryRingExpr {
  override val exprType: RingType = c1.exprType.sum
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = Sum(c1.replaceTypes(vars, overwrite))
  def shred = Sum(c1.shred)
  def renest = Sum(c1.renest)
}


case class Not(c1: RingExpr) extends UnaryRingExpr {
  val exprType = c1.exprType
  override def toString = s"¬${c1.closedString}"
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = Not(c1.replaceTypes(vars, overwrite))
  def shred = Not(c1.shred)
  def renest = Not(c1.renest)
}


case class Negate(c1: RingExpr) extends UnaryRingExpr {
  val exprType = c1.exprType
  override def toString = s"-${c1.closedString}"
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    Negate(c1.replaceTypes(vars, overwrite))
  def shred = Negate(c1.shred)
  def renest = Negate(c1.renest)
}

sealed trait Predicate extends RingExpr with BinaryExpr[KeyExpr,KeyExpr] {
  def opString: String
  override def brackets = ("(",")")
  override def toString = s"$c1 $opString $c2"
}

case class EqualsPredicate(c1: KeyExpr, c2: KeyExpr) extends Predicate {
  val exprType = c1.exprType compareEq c2.exprType
  def opString = "="
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    EqualsPredicate(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))
  def shred = EqualsPredicate(c1.shred, c2.shred)
  def renest = EqualsPredicate(c1.renest, c2.renest)
}

case class IntPredicate(c1: KeyExpr, c2: KeyExpr, p: (Int,Int) => Boolean, opString: String)
                       (implicit val ev1: TypeTag[(Int,Int) => Boolean])
  extends Predicate {
  val exprType = c1.exprType compareOrd c2.exprType
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    IntPredicate(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite), p, opString)
  def shred = IntPredicate(c1.shred, c2.shred, p, opString)
  def renest = IntPredicate(c1.renest, c2.renest, p, opString)
  def literal = reify(p)
}


case class Sng(key: KeyExpr, value: RingExpr) extends LogicalMappingExpr {
  val keyType = key.exprType
  val valueType = value.exprType
  assert(!valueType.isInstanceOf[InfiniteMappingType])
  override def toString = value match {
    case IntExpr(1) => s"sng($key)"
    case _ => s"sng($key, $value)"
  }

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    Sng(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))

  def shred = Sng(key.shred, value.shred)
  def renest = Sng(key.renest, value.renest)
}


sealed trait FromK extends RingExpr with UnaryExpr[KeyExpr]

object FromK {
  def apply(k: KeyExpr): FromK = k.exprType match {
    case BoxedRingType(_) => FromBoxedRing(k)
    case LabelType(_) => FromLabel(k)
    case t => throw InvalidFromKException(s"Cannot create ring expr from key expr with type $t")
  }
}

case class FromBoxedRing(c1: KeyExpr) extends FromK {
  val exprType: RingType = c1.exprType.unbox

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    FromBoxedRing(c1.replaceTypes(vars, overwrite))

  def shred = FromK(c1.shred)
  def renest = ???
}


case class FromLabel(c1: KeyExpr) extends FromK {
  val exprType = c1.exprType match {
    case LabelType(rt) => rt
    case t => throw InvalidFromLabelException(s"Cannot create FromLabel using KeyExpr of type $t.")
  }

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = FromLabel(c1.replaceTypes(vars, overwrite))

  def shred = ???
  def renest = FromK(c1.renest)
}


case class InvalidPredicateException(str: String) extends Exception(str)
case class InvalidFromLabelException(msg: String) extends Exception(msg)
case class VariableResolutionConflictException(msg: String) extends Exception(msg)
case class IllegalResolutionException(msg: String) extends Exception(msg)
case class IllegalFreeVariableRequestException(msg: String) extends Exception(msg)
case class InvalidShreddingException(msg: String) extends Exception(msg)
case class InvalidFromKException(str: String) extends Exception(str)