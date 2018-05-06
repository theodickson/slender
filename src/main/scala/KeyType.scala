package slender

sealed trait KeyType {

  def pair(k: KeyType): KeyType = (this,k) match {
    case (UnresolvedKeyType,_) => UnresolvedKeyType
    case (_,UnresolvedKeyType) => UnresolvedKeyType
    case (l: ResolvedKeyType, r: ResolvedKeyType) => KeyPairType(l,r)
  }

  def triple(k1: KeyType, k2: KeyType): KeyType = (this,k1,k2) match {
    case (UnresolvedKeyType,_,_) | (_,UnresolvedKeyType,_) | (_,_,UnresolvedKeyType) => UnresolvedKeyType
    case (k1: ResolvedKeyType, k2: ResolvedKeyType, k3: ResolvedKeyType) => KeyTuple3Type(k1, k2, k3)
  }

  def -->(r: RingType): RingType = (this,r) match {
    case (UnresolvedKeyType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (k: ResolvedKeyType, r1: ResolvedRingType) => MappingType(k,r1)
  }

  def _1: KeyType = this match {
    case k : Tuple1KeyType => k.t1
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw InvalidKeyProjectionException(s"Cannot project-1 key of type $this.")
  }

  def _2: KeyType = this match {
    case k : Tuple2KeyType => k.t2
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw InvalidKeyProjectionException(s"Cannot project-2 key of type $this.")
  }

  def _3: KeyType = this match {
    case k : Tuple3KeyType => k.t3
    case UnresolvedKeyType => UnresolvedKeyType
    case _ => throw InvalidKeyProjectionException(s"Cannot project-3 key of type $this.")
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

sealed trait Tuple1KeyType extends ResolvedKeyType {
  def t1: ResolvedKeyType
}

sealed trait Tuple2KeyType extends Tuple1KeyType {
  def t2: ResolvedKeyType
}

sealed trait Tuple3KeyType extends Tuple2KeyType {
  def t3: ResolvedKeyType
}

case object UnitType extends ResolvedKeyType

case object IntKeyType extends ResolvedKeyType {
  override def toString = "Int"
}

case object StringKeyType extends ResolvedKeyType {
  override def toString = "String"
}

case class KeyPairType(override val t1: ResolvedKeyType, override val t2: ResolvedKeyType) extends Tuple2KeyType {
  override def toString = s"$t1×$t2"
}

case class KeyTuple3Type(override val t1: ResolvedKeyType,
                         override val t2: ResolvedKeyType,
                         override val t3: ResolvedKeyType) extends Tuple3KeyType {
  override def toString = s"$t1×$t2×$t3"
}

case object LabelType extends ResolvedKeyType

case class BoxedRingType(r: ResolvedRingType) extends ResolvedKeyType {
  override def toString = s"[$r]"
}

case class InvalidKeyProjectionException(msg: String) extends Exception(msg)
case class InvalidUnboxingException(msg: String) extends Exception(msg)