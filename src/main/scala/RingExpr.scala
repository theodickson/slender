package slender

sealed trait RingExpr {
  def ringType: RingType
  def freeVariables: Set[TypedFreeVariable]
}

sealed trait NullaryRingExpr extends RingExpr {
  def freeVariables = Set.empty
}

sealed trait UnaryRingExpr extends RingExpr {
  def child: RingExpr
  def ringType: RingType = child.ringType
  def freeVariables = child.freeVariables
}

sealed trait BinaryRingExpr extends RingExpr {
  def left: RingExpr
  def right: RingExpr
  def freeVariables = left.freeVariables ++ right.freeVariables
}

sealed trait MappingExpr extends RingExpr {
  def keyType: KeyType
  def valueType: RingType
  def ringType: RingType = keyType --> valueType //TODO - could hide errors by not being called on init
}

sealed trait PhysicalMapping extends MappingExpr with NullaryRingExpr

sealed trait LogicalMappingExpr extends MappingExpr {
  def key: KeyExpr
  def value: RingExpr
  def freeVariables = key.freeVariables ++ value.freeVariables
}

case class IntExpr(value: Int) extends NullaryRingExpr {
  val ringType = IntType
  override def toString = s"$value"
}

case class PhysicalCollection(keyType: KeyType, valueType: RingType, ref: String) extends PhysicalMapping {
  override def toString = s"$ref:$ringType"
}

case class PhysicalBag(keyType: KeyType, ref: String) extends PhysicalMapping {
  val valueType = IntType
  override def toString = s"$ref:$ringType"
}

case class InfiniteMappingExpr(key: FreeVariable, value: RingExpr) extends LogicalMappingExpr {
  val keyType = key.keyType
  val valueType = value.ringType
  override def toString = s"{$key => $value}"
}

case class RingPairExpr(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType.pair(right.ringType)
  override def toString = s"⟨$left,$right⟩"
}

case class Project1RingExpr(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType._1
}

case class Project2RingExpr(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType._2
}

case class Add(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType + right.ringType
  override def toString = s"$left + $right"
}

case class Negate(child: RingExpr) extends UnaryRingExpr {
  override def toString = s"-$child"
}

case class Multiply(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType * right.ringType
  override def toString = s"$left * $right"
}

case class Dot(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType dot right.ringType
  override def toString = s"$left ⋅ $right"
}

case class Sum(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType.sum
}

case class Predicate(k1: KeyExpr, k2: KeyExpr) extends RingExpr {
  val ringType = k1.keyType === k2.keyType
  override def toString = s"$k1==$k2"
  def freeVariables = k1.freeVariables ++ k2.freeVariables
}

case class Not(child: RingExpr) extends UnaryRingExpr {
  override def toString = s"¬($child)"
}

case class Sng(key: KeyExpr, value: RingExpr) extends LogicalMappingExpr {
  val keyType = key.keyType
  val valueType = value.ringType
  override def toString = s"($key⟼$value)"
}

sealed trait FromK extends RingExpr

case class FromBoxedRing(k: KeyExpr) extends FromK {
  val ringType: RingType = k.keyType.unbox
  def freeVariables = k.freeVariables
}
//
//case class FromLabel(l: LabelExpr, ctx: ShreddingContext) extends FromK with UnaryRingExpr {
////  val ringType: RingType = l.r.ringType
//  def child = l.r
//}

case class InvalidPredicateException(str: String) extends Exception(str)