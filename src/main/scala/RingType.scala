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
    case (r1,r2) => throw InvalidRingDotException(s"Invalid types for dot operation: $r1 , $r2")
  }

  def *(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (IntType,IntType) => IntType
    case (MappingType(k1,r1),MappingType(k2,r2)) if (k1 == k2) => MappingType(k1, r1.dot(r2))
    case (r1,r2) => throw InvalidRingMultiplyException(s"Invalid types for * operation: $r1, $r2")
  }

  def +(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (r1,r2) if (r1 == r2) => r1
    case (r1,r2) => throw InvalidRingAddException(s"Invalid types for + operation: $r1, $r2")
  }

  def sum: RingType = this match {
    case UnresolvedRingType => UnresolvedRingType
    case MappingType(_,r) => r
    case r => throw InvalidRingSumException(s"Invalid type for sum operation: $r")
  }

  def _1: RingType = this match {
    case UnresolvedRingType => UnresolvedRingType
    case RingPairType(_1, _) => _1
    case r => throw InvalidRingProjectionException(s"Invalid type for projection operation: $r")
  }

  def _2: RingType = this match {
    case UnresolvedRingType => UnresolvedRingType
    case RingPairType(_, _2) => _2
    case r => throw InvalidRingProjectionException(s"Invalid type for projection operation: $r")
  }

}

case object IntType extends RingType {
  override def toString = "Int"
}

protected case class RingPairType(l: RingType, r: RingType) extends RingType {
  override def toString = s"$l×$r"
}

case class MappingType(key: KeyType, ring: RingType) extends RingType {
  override def toString = ring match {
    case IntType => s"Bag($key)"
    case _ => s"$key→$ring"
  }
}

object BagType {
  def apply(keyType: KeyType): RingType = MappingType(keyType, IntType)
}

case object UnresolvedRingType extends RingType {
  override def toString = "Unresolved"
}

case class InvalidRingDotException(msg: String) extends Exception(msg)
case class InvalidRingMultiplyException(msg: String) extends Exception(msg)
case class InvalidRingAddException(msg: String) extends Exception(msg)
case class InvalidRingSumException(msg: String) extends Exception(msg)
case class InvalidRingProjectionException(msg: String) extends Exception(msg)