package slender

sealed trait RingType {

  def pair(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType, _) => UnresolvedRingType
    case (_, UnresolvedRingType) => UnresolvedRingType
    case (r1:ResolvedRingType,r2:ResolvedRingType) => RingPairType(r1,r2)
  }

  def dot(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (r1:ResolvedRingType, r2: ResolvedRingType) => r1.resolvedDot(r2)
  }

  def *(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (IntType,IntType) => IntType
    case (MappingType(k1,r1),MappingType(k2,r2)) if (k1 == k2) => MappingType(k1, r1.resolvedDot(r2))
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

  def box: KeyType = this match {
    case UnresolvedRingType => UnresolvedKeyType
    case r: ResolvedRingType => BoxedRingType(r)
  }


}

case object UnresolvedRingType extends RingType {
  override def toString = "Unresolved"
}

sealed trait ResolvedRingType extends RingType {

  def resolvedDot(other: ResolvedRingType): ResolvedRingType = (this,other) match {
    case (IntType, IntType) => IntType
    case (MappingType(k, r), IntType) => MappingType(k, r.resolvedDot(IntType))
    case (IntType, MappingType(k, r)) => MappingType(k, IntType.resolvedDot(r))
    case (MappingType(k1, r1), MappingType(k2, r2)) => MappingType(KeyPairType(k1,k2), r1.resolvedDot(r2))
    case (r1, r2) => throw InvalidRingDotException(s"Invalid types for dot operation: $r1 , $r2")
  }

}

case object IntType extends ResolvedRingType {
  override def toString = "Int"
}

case class RingPairType(r1: ResolvedRingType, r2: ResolvedRingType) extends ResolvedRingType {
  override def toString = s"$r1×$r2"
}

case class MappingType(k: ResolvedKeyType, r: ResolvedRingType) extends ResolvedRingType {
  override def toString = r match {
    case IntType => s"Bag($k)"
    case _ => s"$k→$r"
  }
}

object BagType {
  def apply(k: ResolvedKeyType): ResolvedRingType = MappingType(k, IntType)
}

case class InvalidRingDotException(msg: String) extends Exception(msg)
case class InvalidRingMultiplyException(msg: String) extends Exception(msg)
case class InvalidRingAddException(msg: String) extends Exception(msg)
case class InvalidRingSumException(msg: String) extends Exception(msg)
case class InvalidRingProjectionException(msg: String) extends Exception(msg)