package slender
//
//sealed trait RingType extends ExprType {
//
//  def dot(r: RingType): RingType = (this,r) match {
//    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType
//    case (r1:ResolvedRingType, r2: ResolvedRingType) => r1.resolvedDot(r2)
//  }
//
//  def *(r: RingType): RingType = (this,r) match {
//
//    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType
//
//    case (IntType,IntType) => IntType
//
//    //If finite on LHS, result is a finite mapping regardless of RHS being finite or infinite::
//    case (FiniteMappingType(k1,r1),m: MappingType) if (k1 == m.k) => FiniteMappingType(k1, r1.resolvedDot(m.r))
//
//    //Infinite on LHS or both infinite currently disallowed:
//
//    //case (m: MappingType,FiniteMappingType(k1,r1)) if (k1 == m.k) => FiniteMappingType(k1, m.r.resolvedDot(r1))
//    //case (InfiniteMappingType(k1,r1),InfiniteMappingType(k2,r2)) if (k1 == k2) =>
//    //  InfiniteMappingType(k1, r1.resolvedDot(r2))
//
//    case (r1:ProductRingType,r2:ProductRingType) if (r1 == r2) => r1
//
//    case (r1,r2) => throw InvalidRingMultiplyException(s"Invalid types for * operation: $r1, $r2")
//  }
//
//  def +(r: RingType): RingType = (this,r) match {
//    case (_ : InfiniteMappingType,_)|(_, _ : InfiniteMappingType) =>
//      throw InvalidRingAddException(s"Cannot add infinite mappings.")
//    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType
//    case (r1,r2) if (r1 == r2) => r1
//    case (r1,r2) => throw InvalidRingAddException(s"Invalid types for + operation: $r1, $r2")
//  }
//
//  def sum: RingType = this match {
//    case UnresolvedRingType => UnresolvedRingType
//    case FiniteMappingType(_,r) => r
//    case r => throw InvalidRingSumException(s"Invalid type for sum operation: $r")
//  }
//
//  def project(n: Int): RingType = this match {
//    case r : ProductRingType => try r.get(n) catch {
//      case _ : java.lang.IndexOutOfBoundsException =>
//        throw InvalidRingProjectionException(s"Cannot project-$n ring of type $this.")
//    }
//    case UnresolvedRingType => UnresolvedRingType
//    case _ => throw InvalidRingProjectionException(s"Cannot project-$n ring of type $this.")
//  }
//
//  def box: KeyType = this match {
//    case UnresolvedRingType => UnresolvedKeyType
//    case r: ResolvedRingType => BoxedRingType(r)
//  }
//
//  def shred: ResolvedRingType
//
//}
//
//case object UnresolvedRingType extends RingType {
//  override def toString = "Unresolved"
//  override def isResolved = false
//  def shred = ???
//}
//
//sealed trait ResolvedRingType extends RingType {
//
//  def resolvedDot(other: ResolvedRingType): ResolvedRingType = (this,other) match {
//
//    case (IntType, IntType) => IntType
//
//    case (FiniteMappingType(k, r), IntType) => FiniteMappingType(k, r.resolvedDot(IntType))
//    case (IntType, FiniteMappingType(k, r)) => FiniteMappingType(k, IntType.resolvedDot(r))
//
//    case (FiniteMappingType(k1, r1), FiniteMappingType(k2, r2)) =>
//      FiniteMappingType(ProductKeyType(k1,k2), r1.resolvedDot(r2))
//
//    //Dot products of infinite mappings currently disallowed:
//
//    //case (InfiniteMappingType(k, r), IntType) => InfiniteMappingType(k, r.resolvedDot(IntType))
//    //case (IntType, InfiniteMappingType(k, r)) => InfiniteMappingType(k, IntType.resolvedDot(r))
//
//    //case (FiniteMappingType(k1, r1), m:MappingType) =>
//    //  FiniteMappingType(KeyPairType(k1,m.k), r1.resolvedDot(m.r))
//
//    //case (m:MappingType, FiniteMappingType(k1, r1)) =>
//    //  FiniteMappingType(KeyPairType(m.k,k1), m.r.resolvedDot(r1))
//
//    //case (InfiniteMappingType(k1,r1),InfiniteMappingType(k2,r2)) =>
//    //  InfiniteMappingType(KeyPairType(k1,k1), r1.resolvedDot(r2))
//
//    //Can dot products with ints by pushing the dot into the elements,
//    //or products with finite mappings by pushing the dot into the values.
//    case (r1:ProductRingType,IntType) => ProductRingType(r1.ts.map(_.resolvedDot(IntType)):_*)
//    case (IntType,r2:ProductRingType) => ProductRingType(r2.ts.map(IntType.resolvedDot(_)):_*)
//    case (r1:ProductRingType,FiniteMappingType(k1,r2)) => FiniteMappingType(k1,r1.resolvedDot(r2))
//    case (FiniteMappingType(k1,r1),r2:ProductRingType) => FiniteMappingType(k1,r1.resolvedDot(r2))
//
//    case (r1, r2) => throw InvalidRingDotException(s"Invalid types for dot operation: $r1 , $r2")
//  }
//
//}
//
//sealed trait PrimitiveRingType extends ResolvedRingType {
//  def shred = this
//}
//
//case object IntType extends PrimitiveRingType {
//  override def toString = "Int"
//}
//
//case class ProductRingType(tts: ResolvedRingType*) extends ResolvedRingType with ProductExprType[ResolvedRingType] {
//  val ts = tts.toSeq
//  override def toString = ts.map(_.closedString).mkString("×")
//  def shred = ProductRingType(ts.map(_.shred) : _ *)
//}
//
//sealed trait MappingType extends ResolvedRingType {
//  def k: ResolvedKeyType
//  def r: ResolvedRingType
//}
//
//case class FiniteMappingType(k: ResolvedKeyType, r: ResolvedRingType) extends MappingType {
//  override def toString = r match {
//    case IntType => s"Bag($k)"
//    case _ => s"$k→$r"
//  }
//  def shred = FiniteMappingType(k.shred, r.shred)
//}
//
//case class InfiniteMappingType(k: ResolvedKeyType, r: ResolvedRingType) extends MappingType {
//  override def toString = s"$k=>$r"
//  def shred = FiniteMappingType(k.shred, r.shred)
//}
//
//object BagType {
//  def apply(k: ResolvedKeyType): ResolvedRingType = FiniteMappingType(k, IntType)
//}
//
//object RingType {
//  def seq(ts: Seq[RingType]): RingType = if (ts.contains(UnresolvedRingType)) UnresolvedRingType else
//    ProductRingType(ts.map(_.asInstanceOf[ResolvedRingType]) : _ *)
//}
//
//case class InvalidRingDotException(msg: String) extends Exception(msg)
//case class InvalidRingMultiplyException(msg: String) extends Exception(msg)
//case class InvalidRingAddException(msg: String) extends Exception(msg)
//case class InvalidRingSumException(msg: String) extends Exception(msg)
//case class InvalidRingProjectionException(msg: String) extends Exception(msg)