package slender

import scala.reflect.runtime.universe._
import definitions._

sealed trait ExprType[T <: ExprType[T]] {

  def isResolved: Boolean = true

  def brackets: (String,String) = ("","")
  def openString: String = toString
  def closedString: String = s"${brackets._1}$openString${brackets._2}"

  def project(n: Int): T = this match {
    case r : ProductExprType[T] => try r.get(n) catch {
      case _ : java.lang.IndexOutOfBoundsException =>
        throw InvalidKeyProjectionException(s"Cannot project-$n expr of type $this.")
    }
    case t : UnresolvedExprType[T] => t.asInstanceOf[T]
    case _ => throw InvalidKeyProjectionException(s"Cannot project-$n expr of type $this.")
  }

  def shred: T
}


sealed trait UnshreddableType[T <: ExprType[T]] extends ExprType[T] {
  def shred: T = ???
}


sealed trait UnresolvedExprType[T <: ExprType[T]] extends ExprType[T] with UnshreddableType[T] {
  override def toString = "Unresolved"
  override def isResolved = false
}


sealed trait ProductExprType[T <: ExprType[T]] extends ExprType[T] {
  def ts: Seq[T]
  def get(n: Int): T = ts(n-1)
  override def brackets = ("(",")")
  override def toString = ts.map(_.closedString).mkString("×")
}






sealed trait KeyType extends ExprType[KeyType] {

  def -->(r: RingType): RingType = (this,r) match {
    case (UnresolvedKeyType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (k: KeyType, r1: RingType) => FiniteMappingType(k,r1)
  }

  def ==>(r: RingType): RingType = (this,r) match { //todo confusing
    case (UnresolvedKeyType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (k: KeyType, r1: RingType) => InfiniteMappingType(k,r1)
  }

  def compareEq(other: KeyType): RingType = (this,other) match {
    case (UnresolvedKeyType, _) | (_, UnresolvedKeyType) => UnresolvedRingType
    case _ => if (this == other) IntType else
      throw InvalidPredicateException(s"Cannot compare keys of differing types $this and $other for equality.")
  }

  def compareOrd(other: KeyType): RingType = (this,other) match {
    case (UnresolvedKeyType,_) | (_,UnresolvedKeyType) => UnresolvedRingType
    case (IntKeyType,IntKeyType) => IntType
    case _ => throw InvalidPredicateException(s"Cannot compare keys of types $this and $other for ordering.")
  }

  def unbox: RingType = this match {
    case UnresolvedKeyType => UnresolvedRingType
    case BoxedRingType(r) => r
    case LabelType(r) => r
    case t => throw InvalidUnboxingException(s"Cannot unbox key expr of non-boxed type $t")
  }

}


case object UnresolvedKeyType extends KeyType with UnresolvedExprType[KeyType]


case class ProductKeyType(ts: List[KeyType]) extends KeyType with ProductExprType[KeyType] {
  def shred = ProductKeyType(ts.map(_.shred))
}


object ProductKeyType {
  def apply(ts: KeyType*): ProductKeyType = ProductKeyType(ts.toList)
}


sealed trait PrimitiveType[T <: ExprType[T]] extends ExprType[T] {
  def tpe: Type
  override def toString = tpe.toString
}

case class PrimitiveKeyType(tpe: Type) extends KeyType with PrimitiveType[KeyType] {
  //todo - should primitive types which can be ring type (e.g Int,Double) be forced to be boxed rings?
  def shred = this
}


case class LabelType(r: RingType) extends KeyType with UnshreddableType[KeyType] {
  override def toString = s"Label($r)"
}


case class BoxedRingType(r: RingType) extends KeyType {
  override def toString = s"[$r]"
  def shred = LabelType(r.shred)
}





sealed trait RingType extends ExprType[RingType] {

  def dot(r: RingType): RingType = (this,r) match {
    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (IntType, IntType) => IntType

    case (FiniteMappingType(k, r), IntType) => FiniteMappingType(k, r.dot(IntType))
    case (IntType, FiniteMappingType(k, r)) => FiniteMappingType(k, IntType.dot(r))

    case (FiniteMappingType(k1, r1), FiniteMappingType(k2, r2)) =>
      FiniteMappingType(ProductKeyType(k1,k2), r1.dot(r2))

    //Dot products of infinite mappings currently disallowed:

    //case (InfiniteMappingType(k, r), IntType) => InfiniteMappingType(k, r.resolvedDot(IntType))
    //case (IntType, InfiniteMappingType(k, r)) => InfiniteMappingType(k, IntType.resolvedDot(r))

    //case (FiniteMappingType(k1, r1), m:MappingType) =>
    //  FiniteMappingType(KeyPairType(k1,m.k), r1.resolvedDot(m.r))

    //case (m:MappingType, FiniteMappingType(k1, r1)) =>
    //  FiniteMappingType(KeyPairType(m.k,k1), m.r.resolvedDot(r1))

    //case (InfiniteMappingType(k1,r1),InfiniteMappingType(k2,r2)) =>
    //  InfiniteMappingType(KeyPairType(k1,k1), r1.resolvedDot(r2))

    //Can dot products with ints by pushing the dot into the elements,
    //or products with finite mappings by pushing the dot into the values.
    case (r1:ProductRingType,IntType) => ProductRingType(r1.ts.map(_.dot(IntType)):_*)
    case (IntType,r2:ProductRingType) => ProductRingType(r2.ts.map(IntType.dot(_)):_*)
    case (r1:ProductRingType,FiniteMappingType(k1,r2)) => FiniteMappingType(k1,r1.dot(r2))
    case (FiniteMappingType(k1,r1),r2:ProductRingType) => FiniteMappingType(k1,r1.dot(r2))

    case (r1, r2) => throw InvalidRingDotException(s"Invalid types for dot operation: $r1 , $r2")
  }

  def *(r: RingType): RingType = (this,r) match {

    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType

    case (IntType,IntType) => IntType

    //If finite on LHS, result is a finite mapping regardless of RHS being finite or infinite::
    case (FiniteMappingType(k1,r1),m: MappingType) if (k1 == m.k) => FiniteMappingType(k1, r1.dot(m.r))

    //Infinite on LHS or both infinite currently disallowed:

    //case (m: MappingType,FiniteMappingType(k1,r1)) if (k1 == m.k) => FiniteMappingType(k1, m.r.resolvedDot(r1))
    //case (InfiniteMappingType(k1,r1),InfiniteMappingType(k2,r2)) if (k1 == k2) =>
    //  InfiniteMappingType(k1, r1.resolvedDot(r2))

    case (r1:ProductRingType,r2:ProductRingType) if (r1 == r2) => r1

    case (r1,r2) => throw InvalidRingMultiplyException(s"Invalid types for * operation: $r1, $r2")
  }

  def +(r: RingType): RingType = (this,r) match {
    case (_ : InfiniteMappingType,_)|(_, _ : InfiniteMappingType) =>
      throw InvalidRingAddException(s"Cannot add infinite mappings.")
    case (UnresolvedRingType,_) | (_,UnresolvedRingType) => UnresolvedRingType
    case (r1,r2) if (r1 == r2) => r1
    case (r1,r2) => throw InvalidRingAddException(s"Invalid types for + operation: $r1, $r2")
  }

  def sum: RingType = this match {
    case UnresolvedRingType => UnresolvedRingType
    case FiniteMappingType(_,r) => r
    case r => throw InvalidRingSumException(s"Invalid type for sum operation: $r")
  }

  def box: KeyType = this match {
    case UnresolvedRingType => UnresolvedKeyType
    case r: RingType => BoxedRingType(r)
  }

}


case object UnresolvedRingType extends RingType with UnresolvedExprType[RingType]


case object IntType extends RingType with PrimitiveType[RingType] {
  override def toString = "Int"
  def tpe = typeOf[Int]
  def shred = this
}


case class ProductRingType(ts: List[RingType]) extends RingType with ProductExprType[RingType] {
  def shred = ProductRingType(ts.map(_.shred))
}


object ProductRingType {
  def apply(ts: RingType*): ProductRingType = ProductRingType(ts.toList)
}


sealed trait MappingType extends RingType {
  def k: KeyType
  def r: RingType
}


case class FiniteMappingType(k: KeyType, r: RingType) extends MappingType {
  override def toString = r match {
    case IntType => s"Bag($k)"
    case _ => s"$k→$r"
  }
  def shred = FiniteMappingType(k.shred, r.shred)
}

object BagType {
  def apply(k: KeyType): RingType = FiniteMappingType(k, IntType)
}


case class InfiniteMappingType(k: KeyType, r: RingType) extends MappingType {
  override def toString = s"$k=>$r"
  def shred = FiniteMappingType(k.shred, r.shred)
}


case class InvalidRingDotException(msg: String) extends Exception(msg)
case class InvalidRingMultiplyException(msg: String) extends Exception(msg)
case class InvalidRingAddException(msg: String) extends Exception(msg)
case class InvalidRingSumException(msg: String) extends Exception(msg)
case class InvalidRingProjectionException(msg: String) extends Exception(msg)
case class InvalidKeyProjectionException(msg: String) extends Exception(msg)
case class InvalidUnboxingException(msg: String) extends Exception(msg)

object definitions {

  val UnitKeyType = PrimitiveKeyType(typeOf[Unit])

  val IntKeyType = PrimitiveKeyType(typeOf[Int])

  val StringKeyType = PrimitiveKeyType(typeOf[String])

}
