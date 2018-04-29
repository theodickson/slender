package slender

sealed trait KeyType {

  type Type

  def pair(k: KeyType): KeyType = (this,k) match {
    case (UnresolvedKeyType,_) => UnresolvedKeyType
    case (_,UnresolvedKeyType) => UnresolvedKeyType
    case (l: ResolvedKeyType, r: ResolvedKeyType) => KeyPair(l,r)
  }

  def -->(r: RingType): RingType = (this,r) match {
    case (UnresolvedKeyType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (k: ResolvedKeyType, r1: ResolvedRingType) => MappingType(k,r1)
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

case object UnresolvedKeyType extends KeyType

sealed trait ResolvedKeyType extends KeyType

case object UnitType extends ResolvedKeyType {
  type Type = Unit
}

case object DomIntType extends ResolvedKeyType {
  type Type = Int
  override def toString = "Int"
}

case object DomStringType extends ResolvedKeyType {
  type Type = String
  override def toString = "String"
}

case class KeyPair(k1: ResolvedKeyType, k2: ResolvedKeyType) extends ResolvedKeyType {
  type Type = (k1.Type, k2.Type)
  override def toString = s"$k1×$k2"
}

case class Label(ref: String) extends ResolvedKeyType

case class BoxedRingType(r: ResolvedRingType) extends ResolvedKeyType {
  override def toString = s"[$r]"
}


case class InvalidKeyProjectionException(msg: String) extends Exception(msg)