package slender

sealed trait RingExpr extends Expr {
  def exprType: RingType
  def replaceTypes(vars: Map[String,ResolvedKeyType], overwrite: Boolean): RingExpr
  def inferTypes(vars: Map[String,ResolvedKeyType]): RingExpr = replaceTypes(vars, false)
  def inferTypes: RingExpr = inferTypes(Map.empty)
  def shred: RingExpr
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
  def exprType: RingType = keyType --> valueType //TODO - could hide errors by not being called on init
}

sealed trait PhysicalMapping extends MappingExpr with NullaryRingExpr {
  def ref: String
  override def toString = s"$ref"
}

case class ShreddedPhysicalMapping(mapping: PhysicalMapping) extends NullaryRingExpr with MappingExpr {
  val keyType = mapping.keyType.shred
  val valueType = mapping.valueType.shred
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
  override def shred: RingExpr = ???
}

case class ShreddedPhysicalBag(keyType: KeyType, ref: String) extends PhysicalMapping {
  val valueType = IntType
  override def shred: RingExpr = ???
}

case class InfiniteMappingExpr(key: KeyExpr, value: RingExpr) extends LogicalMappingExpr {
  assert(key.isInstanceOf[FreeVariable]) //TODO
  val keyType = key.exprType
  val valueType = value.exprType
  override def toString = s"{$key => $value}"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean): RingExpr =
    InfiniteMappingExpr(key.replaceTypes(vars, overwrite), value.replaceTypes(vars, overwrite))

  def shred = InfiniteMappingExpr(key, value.shred)
}

case class RingPairExpr(c1: RingExpr, c2: RingExpr) extends BinaryRingExpr {
  val exprType: RingType = c1.exprType.pair(c2.exprType)
  override def toString = s"⟨$c1,$c2⟩"
  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    RingPairExpr(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))
  def shred = RingPairExpr(c1.shred, c2.shred)
}

case class RingTuple3Expr(c1: RingExpr, c2: RingExpr, c3: RingExpr) extends TernaryRingExpr {
  val exprType = c2.exprType.triple(c2.exprType, c3.exprType)
  override def toString = s"⟨$c2,$c2,$c3⟩"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    RingTuple3Expr(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite), c3.replaceTypes(vars, overwrite))

  def shred = RingTuple3Expr(c1.shred, c2.shred, c3.shred)
}

case class Project1RingExpr(c1: RingExpr) extends UnaryRingExpr {
  override val exprType: RingType = c1.exprType._1

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    Project1RingExpr(c1.replaceTypes(vars, overwrite))

  def shred = Project1RingExpr(c1.shred)
}

case class Project2RingExpr(c1: RingExpr) extends UnaryRingExpr {
  override val exprType: RingType = c1.exprType._2

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    Project2RingExpr(c1.replaceTypes(vars, overwrite))

  def shred = Project2RingExpr(c1.shred)
}

case class Add(c1: RingExpr, c2: RingExpr) extends BinaryRingExpr {
  val exprType: RingType = c1.exprType + c2.exprType
  override def toString = s"($c1 + $c2)"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    Add(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))

  def shred = Add(c1.shred, c2.shred)
}

case class Multiply(c1: RingExpr, c2: RingExpr) extends BinaryRingExpr {
  val exprType: RingType = c1.exprType * c2.exprType
  override def toString = s"($c1 * $c2)"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = {
    val newC1 = c1.replaceTypes(vars, overwrite)
    val newC2 = newC1.exprType match {
      case MappingType(keyType,_) => c2 match {
        case InfiniteMappingExpr(v: FreeVariable,_) => c2.replaceTypes(vars + (v.name -> keyType), overwrite)
        case _ => c2.replaceTypes(vars, overwrite)
      }
      case _ => c2.replaceTypes(vars, overwrite)
    }
    Multiply(newC1, newC2)
  }

  def shred = {
    val newC1 = c1.shred
    val newC2 = newC1.exprType match {
      case MappingType(keyType,_) => c2 match {
        case InfiniteMappingExpr(v: FreeVariable,_) => {
          //println(c2)
          //println(v.name, keyType)
          val replaced = c2.replaceTypes(Map(v.name -> keyType), true)
          //println(replaced)
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
  override def toString = s"($c1 ⋅ $c2)"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    Dot(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))

  def shred = Dot(c1.shred, c2.shred)
}

case class Sum(c1: RingExpr) extends UnaryRingExpr {
  override val exprType: RingType = c1.exprType.sum
  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = Sum(c1.replaceTypes(vars, overwrite))
  def shred = Sum(c1.shred)
  //override def toString = s"Sum(\n\t${c1}\n)"
}

case class Not(c1: RingExpr) extends UnaryRingExpr {
  val exprType = c1.exprType
  override def toString = s"¬($c1)"
  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) = Not(c1.replaceTypes(vars, overwrite))
  def shred = Not(c1.shred)
}

case class Negate(c1: RingExpr) extends UnaryRingExpr {
  val exprType = c1.exprType
  override def toString = s"-$c1"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    Negate(c1.replaceTypes(vars, overwrite))

  def shred = Negate(c1.shred)
}

case class Predicate(c1: KeyExpr, c2: KeyExpr) extends BinaryRingExpr {
  val exprType = c1.exprType === c2.exprType
  override def toString = s"($c1 = $c2)"

  def replaceTypes(vars: Map[String, ResolvedKeyType], overwrite: Boolean) =
    Predicate(c1.replaceTypes(vars, overwrite), c2.replaceTypes(vars, overwrite))

  def shred = Predicate(c1.shred, c2.shred)
}

case class Sng(key: KeyExpr, value: RingExpr) extends LogicalMappingExpr {
  val keyType = key.exprType
  val valueType = value.exprType
  override def toString = s"⟨$key ↦ $value⟩"

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

