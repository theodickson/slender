package slender

sealed trait RingType extends ExprType {

  def pair(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (r1:ResolvedRingType,r2:ResolvedRingType) => RingPairType(r1,r2)
  }

  def triple(k1: RingType, k2: RingType): RingType = (this,k1,k2) match {
    case (UnresolvedRingType,_,_) | (_,UnresolvedRingType,_) | (_,_,UnresolvedRingType) => UnresolvedRingType
    case (k1: ResolvedRingType, k2: ResolvedRingType, k3: ResolvedRingType) => RingTuple3Type(k1, k2, k3)
  }

  def dot(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (r1:ResolvedRingType, r2: ResolvedRingType) => r1.resolvedDot(r2)
  }

  def *(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (IntType,IntType) => IntType
    case (MappingType(k1,r1),MappingType(k2,r2)) if (k1 == k2) => MappingType(k1, r1.resolvedDot(r2))
    case (r1,r2) => throw InvalidRingMultiplyException(s"Invalid types for * operation: $r1, $r2")
  }

  def +(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (r1,r2) if (r1 == r2) => r1
    case (r1,r2) => throw InvalidRingAddException(s"Invalid types for + operation: $r1, $r2")
  }

  def sum: RingType = this match {
    case UnresolvedRingType => UnresolvedRingType
    case MappingType(_,r) => r
    case r => throw InvalidRingSumException(s"Invalid type for sum operation: $r")
  }

  def _1: RingType = this match {
    case r : Tuple2RingType => r.t1
    case r : Tuple3RingType => r.t1
    case UnresolvedRingType => UnresolvedRingType
    case _ => throw InvalidRingProjectionException(s"Cannot project-1 ring of type $this.")
  }

  def _2: RingType = this match {
    case r : Tuple2RingType => r.t2
    case r : Tuple3RingType => r.t2
    case UnresolvedRingType => UnresolvedRingType
    case _ => throw InvalidRingProjectionException(s"Cannot project-2 ring of type $this.")
  }

  def _3: RingType = this match {
    case r : Tuple3RingType => r.t3
    case UnresolvedRingType => UnresolvedRingType
    case _ => throw InvalidRingProjectionException(s"Cannot project-3 ring of type $this.")
  }

  def box: KeyType = this match {
    case UnresolvedRingType => UnresolvedKeyType
    case r: ResolvedRingType => BoxedRingType(r)
  }

  def shred: ResolvedRingType

}

case object UnresolvedRingType extends RingType {
  override def toString = "Unresolved"
  override def isResolved = false
  def shred = ???
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

sealed trait PrimitiveRingType extends ResolvedRingType {
  def shred = this
}

case object IntType extends PrimitiveRingType {
  override def toString = "Int"
}

sealed trait Tuple2RingType extends ResolvedRingType with Tuple2ExprType {
  def t1: ResolvedRingType
  def t2: ResolvedRingType
}

sealed trait Tuple3RingType extends ResolvedRingType with Tuple3ExprType {
  def t1: ResolvedRingType
  def t2: ResolvedRingType
  def t3: ResolvedRingType
}

case class RingPairType(t1: ResolvedRingType, t2: ResolvedRingType) extends Tuple2RingType {
  override def toString = s"${t1.closedString}×${t2.closedString}"
  def shred = RingPairType(t1.shred, t2.shred)
}

case class RingTuple3Type(t1: ResolvedRingType, t2: ResolvedRingType, t3: ResolvedRingType) extends Tuple3RingType {
  override def toString = s"${t1.closedString}×${t2.closedString}×${t3.closedString}"
  def shred = RingTuple3Type(t1.shred, t2.shred, t3.shred)
}

case class MappingType(k: ResolvedKeyType, r: ResolvedRingType) extends ResolvedRingType {
  override def toString = r match {
    case IntType => s"Bag($k)"
    case _ => s"$k→$r"
  }
  def shred = MappingType(k.shred, r.shred)
}

object BagType {
  def apply(k: ResolvedKeyType): ResolvedRingType = MappingType(k, IntType)
}

case class InvalidRingDotException(msg: String) extends Exception(msg)
case class InvalidRingMultiplyException(msg: String) extends Exception(msg)
case class InvalidRingAddException(msg: String) extends Exception(msg)
case class InvalidRingSumException(msg: String) extends Exception(msg)
case class InvalidRingProjectionException(msg: String) extends Exception(msg)