package slender

sealed trait RingType {

  def pair(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType, _) => UnresolvedRingType
    case (_, UnresolvedRingType) => UnresolvedRingType(this, r)
    case _ => RingPairType(this, r)
  }

  def dot(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) => UnresolvedRingType
    case (_,UnresolvedRingType) => UnresolvedRingType
    case (IntType,IntType) => IntType
    case (MappingType(k,r),IntType) => MappingType(k, r.dot(IntType))
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

case object IntType extends RingType

case class RingPairType(l: RingType, r: RingType) extends RingType

case class MappingType(key: KeyType, ring: RingType) extends RingType

case object UnresolvedRingType extends RingType

//unresolved key expressions only appear in the LHS of an InfMapping. They are naturally resolved upon
//introduction in a For expression. In explicit usage of infinite mappings, they are resolved when the












//
//case class CollectionType[K]() extends MappingType(Dom[K](), IntType)


//sealed trait RingType[R <: RingType[R]] {
//  def pair[R1 <: RingType[R1]](other: R1): RingPair[RingType[R], R1] = RingPair(this, other)
//  def dot[R1 <: RingType[R1]](other: R1) = (this,other) match {
//    case (IntType(),IntType()) => IntType()
//    case (CollectionType(k,r),IntType()) => CollectionType(k, r.dot(IntType()))
//    case (CollectionType(k1,r1),CollectionType(k2,r2)) => CollectionType(k1.pair(k2), r1.dot(r2))
//  }
//}
//
//case class IntType() extends RingType[IntType]
//
//case class RingPair[R1 <: RingType[R1], R2 <: RingType[R2]](r1: R1, r2: R2)
//  extends RingType[RingPair[R1, R2]] {
//  def _1: R1 = r1
//  def _2: R2 = r2
//}
//
//case class CollectionType[K <: KeyType[K], R <: RingType[R]](k: K, r: R) extends RingType[CollectionType[R, K]] {
////  def *(c: CollectionType): CollectionType = (this,c) match {
////    case (CollectionType(k1,r1), CollectionType(k2, r2)) if (k1 == k2) => CollectionType(k1, r1.dot(r2))
////  }
//  def sum: R = r
//}
