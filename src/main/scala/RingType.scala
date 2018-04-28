package slender

sealed trait RingType {

  def pair(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType, _) => UnresolvedRingType
    case (_, UnresolvedRingType) => UnresolvedRingType
    case _ => RingPairType(this, r)
  }

  def dot(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (IntType,IntType) => IntType
    case (MappingType(k,r),IntType) => MappingType(k, r.dot(IntType))
    case (IntType, MappingType(k,r)) => MappingType(k, IntType.dot(r))
    case (MappingType(k1,r1),MappingType(k2,r2)) => MappingType(k1.pair(k2), r1.dot(r2))
    case (r1,r2) => throw new IllegalArgumentException(s"Invalid ring types for dot operation: $r1 . $r2")
  }

  def *(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (IntType,IntType) => IntType
    case (MappingType(k1,r1),MappingType(k2,r2)) if (k1 == k2) => MappingType(k1, r1.dot(r2))
    case _ => throw new IllegalArgumentException("Invalid ring types for * operation.")
  }

  def +(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (r1,r2) if (r1 == r2) => r1
    case _ => throw new IllegalArgumentException("Can only add ring values of the same type")
  }

  def sum: RingType = this match {
    case UnresolvedRingType => UnresolvedRingType
    case MappingType(_,r) => r
    case _ => throw new IllegalArgumentException("Only MappingType ring expressions may be summed.")
  }

  def _1: RingType = this match {
    case UnresolvedRingType => UnresolvedRingType
    case RingPairType(_1, _) => _1
    case _ => throw new IllegalArgumentException("Cannot project non-pair type rings")
  }

  def _2: RingType = this match {
    case UnresolvedRingType => UnresolvedRingType
    case RingPairType(_, _2) => _2
    case _ => throw new IllegalArgumentException("Cannot project non-pair type rings")
  }

}

case object IntType extends RingType {
  override def toString = "Int"
}

case class RingPairType(l: RingType, r: RingType) extends RingType {
  override def toString = s"($l × $r)"
}

case class MappingType(key: KeyType, ring: RingType) extends RingType {
  override def toString = ring match {
    case IntType => s"Bag($key)"
    case _ => s"($key → $ring)"
  }
}

object BagType {
  def apply(keyType: KeyType): RingType = MappingType(keyType, IntType)
}

case object UnresolvedRingType extends RingType {
  override def toString = "Unresolved"
}