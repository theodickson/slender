package slender

sealed trait KeyType {

  def pair(k: KeyType): KeyType = (this,k) match {
    case (UnresolvedKeyType,_) => UnresolvedKeyType
    case (_,UnresolvedKeyType) => UnresolvedKeyType
    case (l: ResolvedKeyType, r: ResolvedKeyType) => KeyPairType(l,r)
  }

  def -->(r: RingType): RingType = (this,r) match {
    case (UnresolvedKeyType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (k: ResolvedKeyType, r1: ResolvedRingType) => MappingType(k,r1)
  }

  def _1: KeyType = this match {
    case KeyPairType(l,_) => l
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw InvalidKeyProjectionException("Cannot project non-pair type key.")
  }

  def _2: KeyType = this match {
    case KeyPairType(_,r) => r
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw InvalidKeyProjectionException("Cannot project non-pair type key.")
  }

  def ===(other: KeyType): RingType = (this,other) match {
    case (UnresolvedKeyType,_) => UnresolvedRingType
    case (_,UnresolvedKeyType) => UnresolvedRingType
    case _ => if (this == other) IntType else
      throw InvalidPredicateException(s"Cannot compare keys of differing type $this and $other for equality.")
  }

  def unbox: RingType = this match {
    case UnresolvedKeyType => UnresolvedRingType
    case r: BoxedRingType => r.r
    case t => throw InvalidUnboxingException(s"Cannot unbox key expr of non-boxed type $t")
  }

}

case object UnresolvedKeyType extends KeyType

sealed trait ResolvedKeyType extends KeyType

case object UnitType extends ResolvedKeyType

case object IntKeyType extends ResolvedKeyType {
  override def toString = "Int"
}

case object StringKeyType extends ResolvedKeyType {
  override def toString = "String"
}

case class KeyPairType(k1: ResolvedKeyType, k2: ResolvedKeyType) extends ResolvedKeyType {
  override def toString = s"$k1×$k2"
}

case object LabelType extends ResolvedKeyType

case class BoxedRingType(r: ResolvedRingType) extends ResolvedKeyType {
  override def toString = s"[$r]"
}

case class InvalidKeyProjectionException(msg: String) extends Exception(msg)
case class InvalidUnboxingException(msg: String) extends Exception(msg)