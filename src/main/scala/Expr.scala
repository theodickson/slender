package slender

import definitions._

import scala.collection.mutable.StringBuilder
import scala.reflect.runtime.universe._


sealed trait Expr[T <: Expr[T]] {

  def exprType: ExprType[_]

  def children: Seq[Expr[_]]

  def shred: T

  def replaceTypes(vars: Map[String,KeyType], overwrite: Boolean): T
  def inferTypes(vars: Map[String,KeyType]): T = replaceTypes(vars, false)
  def inferTypes: T = inferTypes(Map.empty)

  def variables: Set[VariableKeyExpr] =
    children.foldRight(Set.empty[VariableKeyExpr])((v, acc) => acc ++ v.variables)
  def freeVariables: Set[VariableKeyExpr] =
    children.foldRight(Set.empty[VariableKeyExpr])((v, acc) => acc ++ v.freeVariables)
  def labelDefinitions: Seq[String] =
    children.foldLeft(Seq.empty[String])((acc,v) => acc ++ v.labelDefinitions)

  def isShredded: Boolean = children.exists(_.isShredded)
  def isResolved: Boolean = exprType.isResolved
  def isComplete: Boolean = freeVariables.isEmpty

  def leftBracket = ""
  def rightBracket = ""
  def openString = toString
  def closedString = s"$leftBracket$openString$rightBracket"

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


sealed trait NullaryExpr {
  def children = List.empty[Expr[_]]
}


sealed trait UnaryExpr[T <: Expr[T]] {
  def c1: Expr[T]
  def children = List(c1)
}


trait BinaryExpr[T1 <: Expr[T1], T2 <: Expr[T2]] {
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
  def shred: KeyExpr = this match {
    case BoxedRingExpr(r) => LabelExpr(r.shred)
    case _ => shredR
  }
  def shredR: KeyExpr
}


sealed trait NullaryKeyExpr extends KeyExpr with NullaryExpr {
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = this
  def shredR: KeyExpr = this
}


case object UnitKeyExpr extends NullaryKeyExpr { //todo - need this or just make companion objects?
  val exprType = UnitKeyType
  override def toString = "Unit"
}


case class IntKeyExpr(i: Int) extends NullaryKeyExpr {
  val exprType = IntKeyType
  override def toString = s"$i"
}


case class StringKeyExpr(s: String) extends NullaryKeyExpr {
  val exprType = StringKeyType
  override def toString = s""""$s""""
}


case class PrimitiveKeyExpr[T : TypeTag](t: T) extends NullaryKeyExpr {
  val exprType = PrimitiveKeyType(typeOf[T])
  override def toString = t.toString
}


case class KeyProductExpr(children: List[KeyExpr]) extends KeyExpr with ProductExpr[KeyExpr] {
  val exprType = if (children.map(_.exprType).forall(_.isResolved))
    ProductKeyType(children.map(_.exprType)) else UnresolvedKeyType

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    KeyProductExpr(children.map(_.replaceTypes(vars, overwrite)))

  def shredR = KeyProductExpr(children.map(_.shred))
}

object KeyProductExpr {
  def apply(exprs: KeyExpr*) = new KeyProductExpr(exprs.toList)
}


case class ProjectKeyExpr(c1: KeyExpr, n: Int) extends KeyExpr with ProjectExpr[KeyExpr] with UnaryExpr[KeyExpr] {
  val exprType = c1.exprType.project(n)
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    ProjectKeyExpr(c1.replaceTypes(vars, overwrite), n)
  def shredR = ProjectKeyExpr(c1.shred, n)
  override def toString = c1 match {
    case TypedVariableKeyExpr(name, kt) => s""""$name"._$n"""
    case UntypedVariableKeyExpr(name) => s""""$name._$n:?"""
    case _ => s"$c1._$n"
  }
}


sealed trait VariableKeyExpr extends NullaryKeyExpr {
  def name: String
  override def variables = Set(this)
  override def freeVariables = Set(this)
}


case class TypedVariableKeyExpr(name: String, exprType: KeyType) extends VariableKeyExpr {
  override def toString = s""""$name""""
  override def explain: String = s""""$name": $exprType"""
  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = vars.get(name) match {
    case None | Some(`exprType`) => this
    case Some(otherType) => if (overwrite) TypedVariableKeyExpr(name, otherType) else
      throw VariableResolutionConflictException(
        s"Tried to resolve var $name with type $otherType, already had type $exprType, overwriting false."
      )
  }
}


case class UntypedVariableKeyExpr(name: String) extends VariableKeyExpr {
  val exprType = UnresolvedKeyType
  override def explain = toString
  override def toString = s""""$name": ?"""
  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = vars.get(name) match {
    case None => this
    case Some(eT) => TypedVariableKeyExpr(name, eT)
  }
}


sealed trait ToK extends KeyExpr with UnaryExpr[RingExpr]


case class BoxedRingExpr(c1: RingExpr) extends ToK {
  val exprType = c1.exprType.box
  override def toString = s"[$c1]"
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    BoxedRingExpr(c1.replaceTypes(vars, overwrite))
  def shredR = throw new IllegalStateException("Cannot call shredR on BoxedRingExpr")
}


case class LabelExpr(c1: RingExpr) extends ToK {

  val exprType = c1.exprType match {
    case UnresolvedRingType => throw new IllegalStateException("Cannot create labeltype from unresolved ring type.")
    case rt: RingType => LabelType(rt)
  }

  private def id = hashCode.abs.toString.take(3).toInt

  override def toString = s"Label($id)"

  def explainFreeVariables = freeVariables.map(_.explain).mkString("\n\t")

  def definition: String = {
    val s = new StringBuilder(s"$this: ${c1.exprType}")
    s ++= s"\n\n\t$c1"
    s ++= s"\n\n\tWhere:\n\t$explainFreeVariables\n"
    //    s ++= s"\n\tWrapped expr:\n\t$c1"
    s.mkString
  }
  override def labelDefinitions = c1.labelDefinitions :+ definition

  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    LabelExpr(c1.replaceTypes(vars, overwrite))

  override def shredR =
    throw InvalidShreddingException("Cannot shred a LabelExpr, it's already shredded.")

  override def isShredded = true
}






sealed trait RingExpr extends Expr[RingExpr] {
  def exprType: RingType
  def shred: RingExpr
  def shredAndExplain: String = {
    explain + "\n" + shred.explain + "\n\n"
  }
}


sealed trait NullaryRingExpr extends RingExpr with NullaryExpr {
  def replaceTypes(vars: Map[String,KeyType], overwrite: Boolean): RingExpr = this
  def shred: RingExpr = this
}


sealed trait UnaryRingExpr extends RingExpr with UnaryExpr[RingExpr]


sealed trait BinaryRingExpr extends RingExpr with BinaryExpr[RingExpr,RingExpr]


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


case class IntExpr(value: Int) extends NullaryRingExpr {
  val exprType = IntType
  override def toString = s"$value"
}


case class PhysicalCollection(keyType: KeyType, valueType: RingType, ref: String) extends PhysicalMapping {
  override def shred: RingExpr = ShreddedPhysicalCollection(keyType.shred, valueType.shred, ref)
}


case class PhysicalBag(keyType: KeyType, ref: String) extends PhysicalMapping {
  val valueType = IntType
  override def shred: RingExpr = ShreddedPhysicalBag(keyType.shred, ref)
}


case class ShreddedPhysicalCollection(keyType: KeyType, valueType: RingType, ref: String) extends PhysicalMapping {
  override def toString = s"${ref}_F"
  override def shred: RingExpr = ???
}


case class ShreddedPhysicalBag(keyType: KeyType, ref: String) extends PhysicalMapping {
  val valueType = IntType
  override def toString = s"${ref}_F"
  override def shred: RingExpr = ???
}


case class InfiniteMappingExpr(key: KeyExpr, value: RingExpr) extends LogicalMappingExpr {
  assert(key.isInstanceOf[VariableKeyExpr]) //TODO
  val keyType = key.exprType
  val valueType = value.exprType
  override val exprType = keyType ==> valueType
  override def toString = s"{$key => $value}"

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean): RingExpr =
    InfiniteMappingExpr(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))

  def shred = InfiniteMappingExpr(key, value.shred)
}


case class RingProductExpr(children: List[RingExpr]) extends RingExpr with ProductExpr[RingExpr] {
  val exprType = if (children.map(_.exprType).forall(_.isResolved))
    ProductRingType(children.map(_.exprType)) else UnresolvedRingType
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    RingProductExpr(children.map(_.replaceTypes(vars, overwrite)))
  def shred = RingProductExpr(children.map(_.shred))
}

object RingProductExpr {
  def apply(exprs: RingExpr*) = new RingProductExpr(exprs.toList)
}


case class ProjectRingExpr(c1: RingExpr, n: Int) extends RingExpr with ProjectExpr[RingExpr] with UnaryExpr[RingExpr] {
  val exprType = c1.exprType.project(n)
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    ProjectRingExpr(c1.replaceTypes(vars, overwrite), n)
  def shred = ProjectRingExpr(c1.shred, n)
}


case class Add(c1: RingExpr, c2: RingExpr) extends BinaryRingExpr {
  val exprType: RingType = c1.exprType + c2.exprType
  override def leftBracket = "("; override def rightBracket = ")"
  override def toString = s"${c1.closedString} + ${c2.closedString}"

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    Add(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))

  def shred = Add(c1.shred, c2.shred)
}


case class Multiply(c1: RingExpr, c2: RingExpr) extends BinaryRingExpr {
  val exprType: RingType = c1.exprType * c2.exprType
  override def leftBracket = "("; override def rightBracket = ")"
  override def toString = s"${c1.closedString} * ${c2.closedString}"

  override def freeVariables = c2 match {
    case InfiniteMappingExpr(k: VariableKeyExpr, _) => c1.freeVariables ++ c2.freeVariables - k
    case _ => c1.freeVariables ++ c2.freeVariables
  }

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = {
    val newC1 = c1.replaceTypes(vars, overwrite)
    val newC2 = newC1.exprType match {
      case FiniteMappingType(keyType,_) => c2 match {
        case InfiniteMappingExpr(v: VariableKeyExpr,_) => c2.replaceTypes(vars + (v.name -> keyType), overwrite)
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
          val replaced = c2.replaceTypes(Map(v.name -> keyType), true)
          replaced.shred
        }
        case _ => c2.shred
      }
      case _ => c2.shred
    }
    Multiply(newC1, newC2)
  }
}


case class Dot(c1: RingExpr, c2: RingExpr) extends BinaryRingExpr {
  val exprType: RingType = c1.exprType dot c2.exprType
  override def leftBracket = "("; override def rightBracket = ")"
  override def toString = s"${c1.closedString} ⊙ ${c2.closedString}"

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    Dot(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))

  def shred = Dot(c1.shred, c2.shred)
}


case class Sum(c1: RingExpr) extends UnaryRingExpr {
  override val exprType: RingType = c1.exprType.sum
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = Sum(c1.replaceTypes(vars, overwrite))
  def shred = Sum(c1.shred)
}


case class Not(c1: RingExpr) extends UnaryRingExpr {
  val exprType = c1.exprType
  override def toString = s"¬${c1.closedString}"
  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = Not(c1.replaceTypes(vars, overwrite))
  def shred = Not(c1.shred)
}


case class Negate(c1: RingExpr) extends UnaryRingExpr {
  val exprType = c1.exprType
  override def toString = s"-${c1.closedString}"

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    Negate(c1.replaceTypes(vars, overwrite))

  def shred = Negate(c1.shred)
}


case class Predicate(c1: KeyExpr, c2: KeyExpr) extends RingExpr with BinaryExpr[KeyExpr,KeyExpr] {
  val exprType = c1.exprType === c2.exprType
  override def leftBracket = "("; override def rightBracket = ")"
  override def toString = s"$c1 = $c2"

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    Predicate(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))

  def shred = Predicate(c1.shred, c2.shred)
}


case class Sng(key: KeyExpr, value: RingExpr) extends LogicalMappingExpr {
  val keyType = key.exprType
  val valueType = value.exprType
  assert(!valueType.isInstanceOf[InfiniteMappingType])
  val exprType = keyType --> valueType
  override def toString = value match {
    case IntExpr(1) => s"sng($key)"
    case _ => s"sng($key, $value)"
  }

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    Sng(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))

  def shred = Sng(key.shred, value.shred)
}


sealed trait FromK extends RingExpr with UnaryExpr[KeyExpr]


case class FromBoxedRing(c1: KeyExpr) extends FromK {
  val exprType: RingType = c1.exprType.unbox

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
    FromBoxedRing(c1.replaceTypes(vars, overwrite))

  def shred = FromLabel(c1.shred)
}


case class FromLabel(c1: KeyExpr) extends FromK {
  val exprType = c1.exprType match {
    case LabelType(rt) => rt
    case t => throw InvalidFromLabelException(s"Cannot create FromLabel using KeyExpr of type $t.")
  }

  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = FromLabel(c1.replaceTypes(vars, overwrite))

  def shred = throw InvalidShreddingException("Cannot shred FromLabel expr, it's already shredded.")
}


case class InvalidPredicateException(str: String) extends Exception(str)
case class InvalidFromLabelException(msg: String) extends Exception(msg)
case class VariableResolutionConflictException(msg: String) extends Exception(msg)
case class IllegalResolutionException(msg: String) extends Exception(msg)
case class IllegalFreeVariableRequestException(msg: String) extends Exception(msg)
case class InvalidShreddingException(msg: String) extends Exception(msg)