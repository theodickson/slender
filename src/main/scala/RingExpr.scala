package slender

sealed trait RingExpr extends Expr {
  def exprType: RingType
  def replaceTypes(vars: Map[String,ResolvedKeyType], overwrite: Boolean): RingExpr
  def inferTypes(vars: Map[String,ResolvedKeyType]): RingExpr = replaceTypes(vars, false)
  def inferTypes: RingExpr = inferTypes(Map.empty)
  def shred: RingExpr
  def shredAndExplain: String = {
    explain + "\n" + shred.explain + "\n\n"
  }
}

sealed trait NullaryRingExpr extends RingExpr with NullaryExpr {
  def replaceTypes(vars: Map[String,ResolvedKeyType], overwrite: Boolean): RingExpr = this
  def shred: RingExpr = this
}

sealed trait UnaryRingExpr extends RingExpr with UnaryExpr

sealed trait BinaryRingExpr extends RingExpr with BinaryExpr

sealed trait TernaryRingExpr extends RingExpr with TernaryExpr

sealed trait MappingExpr extends RingExpr {
  def keyType: KeyType
  def valueType: RingType
}

sealed trait PhysicalMapping extends MappingExpr with NullaryRingExpr {
  def ref: String
  override def toString = s"$ref"
  def exprType: RingType = keyType --> valueType //TODO - could hide errors by not being called on init
}

sealed trait LogicalMappingExpr extends MappingExpr with BinaryExpr {
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
  val exprType = keyType ==> valueType
  override def toString = s"{$key => $value}"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean): RingExpr =
    InfiniteMappingExpr(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))

  def shred = InfiniteMappingExpr(key, value.shred)
}

case class RingProductExpr(cs: RingExpr*) extends RingExpr {
  val children = cs.toSeq
  val exprType = RingType.seq(children.map(_.exprType))
  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    RingProductExpr(children.map(_.replaceTypes(vars, overwrite)) : _ *)
  def shred = RingProductExpr(children.map(_.shred) : _ *)
  override def toString = s"⟨${children.mkString(",")}⟩"
}


case class ProjectRingExpr(n: Int)(val c1: RingExpr) extends UnaryRingExpr {
  override val exprType: RingType = c1.exprType.project(n)

  override def toString = s"$c1._$n"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    ProjectRingExpr(n)(c1.replaceTypes(vars, overwrite))

  def shred = ProjectRingExpr(n)(c1.shred)
}


case class Add(c1: RingExpr, c2: RingExpr) extends BinaryRingExpr {
  val exprType: RingType = c1.exprType + c2.exprType
  override def leftBracket = "("; override def rightBracket = ")"
  override def toString = s"${c1.closedString} + ${c2.closedString}"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
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

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = {
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

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    Dot(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))

  def shred = Dot(c1.shred, c2.shred)
}

case class Sum(c1: RingExpr) extends UnaryRingExpr {
  override val exprType: RingType = c1.exprType.sum
  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = Sum(c1.replaceTypes(vars, overwrite))
  def shred = Sum(c1.shred)
}

case class Not(c1: RingExpr) extends UnaryRingExpr {
  val exprType = c1.exprType
  override def toString = s"¬${c1.closedString}"
  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = Not(c1.replaceTypes(vars, overwrite))
  def shred = Not(c1.shred)
}

case class Negate(c1: RingExpr) extends UnaryRingExpr {
  val exprType = c1.exprType
  override def toString = s"-${c1.closedString}"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    Negate(c1.replaceTypes(vars, overwrite))

  def shred = Negate(c1.shred)
}

case class Predicate(c1: KeyExpr, c2: KeyExpr) extends BinaryRingExpr {
  val exprType = c1.exprType === c2.exprType
  override def leftBracket = "("; override def rightBracket = ")"
  override def toString = s"$c1 = $c2"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
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

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    Sng(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))

  def shred = Sng(key.shred, value.shred)
}

sealed trait FromK extends UnaryRingExpr

case class FromBoxedRing(c1: KeyExpr) extends FromK {
  val exprType: RingType = c1.exprType.unbox

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    FromBoxedRing(c1.replaceTypes(vars, overwrite))

  def shred = FromLabel(c1.shred)
}

case class FromLabel(c1: KeyExpr) extends FromK {
  val exprType = c1.exprType match {
    case LabelType(rt) => rt
    case t => throw InvalidFromLabelException(s"Cannot create FromLabel using KeyExpr of type $t.")
  }

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = FromLabel(c1.replaceTypes(vars, overwrite))

  def shred = throw InvalidShreddingException("Cannot shred FromLabel expr, it's already shredded.")
}

case class InvalidPredicateException(str: String) extends Exception(str)
case class InvalidFromLabelException(msg: String) extends Exception(msg)

