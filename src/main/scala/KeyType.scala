package slender

sealed trait KeyType {

  type Type

  def pair(k: KeyType): KeyType = (this,k) match {
    case (UnresolvedKeyType,_) => UnresolvedKeyType
    case (_,UnresolvedKeyType) => UnresolvedKeyType
    case _ => KeyPair(this,k)
  }

  def -->(r: RingType): RingType = (this,r) match {
    case (UnresolvedKeyType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case _ => MappingType(this, r)
  }

  def _1: KeyType = this match {
    case KeyPair(l,_) => l
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw InvalidKeyProjectionException("Cannot project non-pair type key.")
  }

  def _2: KeyType = this match {
    case KeyPair(_,r) => r
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw InvalidKeyProjectionException("Cannot project non-pair type key.")
  }

  def ===(other: KeyType): RingType = (this,other) match {
    case (UnresolvedKeyType,_) => UnresolvedRingType
    case (_,UnresolvedKeyType) => UnresolvedRingType
    case _ => if (this == other) IntType else
      throw InvalidPredicateException(s"Cannot compare keys of differing type $this and $other for equality.")
  }

}

case object UnitType extends KeyType {
  type Type = Unit
}

case object DomIntType extends KeyType {
  type Type = Int
  override def toString = "Int"
}

case object DomStringType extends KeyType {
  type Type = String
  override def toString = "String"
}

case class KeyPair(k1: KeyType, k2: KeyType) extends KeyType {
  if (k1 == UnresolvedKeyType || k2 == UnresolvedKeyType)
    throw InvalidKeyPairException("Cannot create KeyPair type with unresolved key typed arguments.")
  type Type = (k1.Type, k2.Type)
  override def toString = s"$k1×$k2"
}

case class Label(ref: String) extends KeyType

case class BoxedRingType(r: RingType) extends KeyType {
  if (r == UnresolvedRingType) throw InvalidBoxedRingException(
    "Cannot create BoxedRingType with unresolved ring argument"
  )
  override def toString = s"[$r]"
}

case object UnresolvedKeyType extends KeyType

case class InvalidKeyProjectionException(msg: String) extends Exception(msg)
case class InvalidKeyPairException(msg: String) extends Exception(msg)
case class InvalidBoxedRingException(msg: String) extends Exception(msg)