package slender

sealed trait RingExpr {
  def ringType: RingType
  def freeVariables: Set[TypedFreeVariable]
  def shred: RingExpr = this
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
  override def shred = this //TODO
}

case class PhysicalBag(keyType: KeyType, ref: String) extends PhysicalMapping {
  val valueType = IntType
  override def toString = s"$ref:$ringType"
  override def shred = this //TODO
}

case class InfiniteMappingExpr(key: FreeVariable, value: RingExpr) extends LogicalMappingExpr {
  val keyType = key.keyType
  val valueType = value.ringType
  override def toString = s"{$key => $value}"
  override def shred = InfiniteMappingExpr(key, value.shred)
}

case class RingPairExpr(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType.pair(right.ringType)
  override def toString = s"⟨$left,$right⟩"
  override def shred = RingPairExpr(left.shred, right.shred)
}

case class RingTuple3Expr(k1: RingExpr, k2: RingExpr, k3: RingExpr) extends RingExpr {
  val ringType = k2.ringType.triple(k2.ringType, k3.ringType)
  override def toString = s"⟨$k2,$k2,$k3⟩"
  def freeVariables = k1.freeVariables ++ k2.freeVariables ++ k3.freeVariables
  override def shred = RingTuple3Expr(k1.shred, k2.shred, k3.shred)
}

case class Project1RingExpr(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType._1
  override def shred = Project1RingExpr(child.shred)
}

case class Project2RingExpr(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType._2
  override def shred = Project2RingExpr(child.shred)
}

case class Add(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType + right.ringType
  override def toString = s"$left + $right"
  override def shred = Add(left.shred, right.shred)
}

case class Negate(child: RingExpr) extends UnaryRingExpr {
  override def toString = s"-$child"
  override def shred = Negate(child.shred)
}

case class Multiply(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType * right.ringType
  override def toString = s"$left * $right"
  override def shred = Multiply(left.shred, right.shred)
}

case class Dot(left: RingExpr, right: RingExpr) extends BinaryRingExpr {
  val ringType: RingType = left.ringType dot right.ringType
  override def toString = s"$left ⋅ $right"
  override def shred = Dot(left.shred, right.shred)
}

case class Sum(child: RingExpr) extends UnaryRingExpr {
  override val ringType: RingType = child.ringType.sum
  override def shred = Sum(child.shred)
}

case class Predicate(k1: KeyExpr, k2: KeyExpr) extends RingExpr {
  val ringType = k1.keyType === k2.keyType
  override def toString = s"$k1==$k2"
  def freeVariables = k1.freeVariables ++ k2.freeVariables
  override def shred = Predicate(k1.shred, k2.shred)
}

case class Not(child: RingExpr) extends UnaryRingExpr {
  override def toString = s"¬($child)"
  override def shred = Not(child.shred)
}

case class Sng(key: KeyExpr, value: RingExpr) extends LogicalMappingExpr {
  val keyType = key.keyType
  val valueType = value.ringType
  override def toString = s"($key⟼$value)"
  override def shred = Sng(key.shred, value.shred)
}

sealed trait FromK extends RingExpr

case class FromBoxedRing(k: KeyExpr) extends FromK {
  val ringType: RingType = k.keyType.unbox
  def freeVariables = k.freeVariables
  override def shred = FromLabel(k.shred)
}

case class FromLabel(k: KeyExpr) extends FromK {
  val ringType = k match {
    case l : LabelExpr => l.r.ringType
    case _ => throw new IllegalStateException()
  }
  override def shred = throw InvalidShreddingException("Cannot shred FromLabel expr, it's already shredded.")
  override def freeVariables = k.freeVariables
}

case class InvalidPredicateException(str: String) extends Exception(str)