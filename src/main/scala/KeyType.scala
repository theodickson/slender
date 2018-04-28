package slender

sealed trait KeyType {
  type Type
  def pair(k: KeyType): KeyType = KeyPair(this, k)
  def -->(r: RingType): RingType = (this,r) match {
    case (UnresolvedKeyType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case _ => MappingType(this, r)
  }
  def _1: KeyType = this match {
    case KeyPair(l,_) => l
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw new IllegalArgumentException("Cannot project non-pair type key.")
  }
  def _2: KeyType = this match {
    case KeyPair(_,r) => r
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw new IllegalArgumentException("Cannot project non-pair type key.")
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
  type Type = (k1.Type, k2.Type)
  override def toString = s"($k1 × $k2)"
}

case class Label(ref: String) extends KeyType

case class BoxedRingType(r: RingType) extends KeyType {
  override def toString = s"[$r]"
}

case object UnresolvedKeyType extends KeyType