package slender

//TODO - unresolved type handling in pairs etc.

sealed trait KeyType {
  type Type
  def pair(k: KeyType): KeyType = KeyPair(this, k)
}

case object UnitType extends KeyType {
  type Type = Unit
}

case object DomIntType extends KeyType {
  type Type = Int
}

case object DomStringType extends KeyType {
  type Type = String
}

case class KeyPair(k1: KeyType, k2: KeyType) extends KeyType {
  type Type = (k1.Type, k2.Type)
  def _1: KeyType = k1
  def _2: KeyType = k2
}

case class Label(ref: String) extends KeyType

case class BoxedRing(r: RingType) extends KeyType

case object UnresolvedKeyType extends KeyType
//sealed trait KeyType[T <: KeyType[T]] {
//  def pair[T1 <: KeyType[T1]](k: KeyType[T1]): KeyPair[KeyType[T], KeyType[T1]] = KeyPair(this, k)
//}
//case class UnitType() extends KeyType[UnitType]
//case class Dom[T]() extends KeyType[Dom[T]]
//case class KeyPair[T1 <: KeyType[T1], T2 <: KeyType[T2]](k1: T1, k2: T2)
//  extends KeyType[KeyPair[T1, T2]] {
//  def _1: T1 = k1
//  def _2: T2 = k2
//}


//object KeyType {
//  def apply[T](implicit tag: ClassTag[T]): KeyType = tag.runtimeClass match {
//    case _ : Class[Unit] => UnitType
//    case _ : Class[(_,_)]=>
//  }
//}