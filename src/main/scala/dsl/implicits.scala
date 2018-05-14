package slender.dsl

import slender._
import scala.reflect.runtime.universe._

object implicits {

  implicit class StringImplicits(s: String) {

    def <--(r: RingExpr): (VariableKeyExpr, RingExpr) = (UntypedVariableKeyExpr(s),r)
    def -->(r: RingExpr): YieldPair = YieldPair(UntypedVariableKeyExpr(s),r)
    //since implicit methods on result of implicit conversions dont tend to resolve,
    //duplicate keyexpr implicits here:
    def _1: KeyExpr = ProjectKeyExpr(s, 1)
    def _2: KeyExpr = ProjectKeyExpr(s, 2)
    def _3: KeyExpr = ProjectKeyExpr(s, 3)

    def ===(other: KeyExpr): RingExpr = EqualsPredicate(s, other)
    def ===(other: String): RingExpr = EqualsPredicate(s, other)
    def =!=(other: KeyExpr): RingExpr = Not(EqualsPredicate(s,other))
    def =!=(other: String): RingExpr = Not(EqualsPredicate(s,other))
//    def >(other: KeyExpr): RingExpr = IntPredicate(s, other, _ > _, ">")
//    def <(other: KeyExpr): RingExpr = IntPredicate(s, other, _ < _, "<")

    def ==>(r: RingExpr): InfiniteMappingExpr = InfiniteMappingExpr(s, r)
  }


  implicit class RingExprImplicits(r: RingExpr) {
    def +(r1: RingExpr) = Add(r, r1)
    def *(r1: RingExpr) = Multiply(r, r1)

    def dot(r1: RingExpr) = Dot(r, r1)
    def unary_- = Negate(r)
    def unary_! = Not(r)

    def _1: RingExpr = ProjectRingExpr(r, 1)
    def _2: RingExpr = ProjectRingExpr(r, 2)

    def && = * _
    def || = this.+ _

    def isTyped: Boolean = r.exprType != UnresolvedRingType

  }

  implicit def boolToInt(b: Boolean): Int = if (b) 1 else 0

  implicit class KeyExprImplicits(k: KeyExpr) {
    def _1: KeyExpr = ProjectKeyExpr(k, 1)
    def _2: KeyExpr = ProjectKeyExpr(k, 2)
    def _3: KeyExpr = ProjectKeyExpr(k, 3)

    def ===(other: KeyExpr): RingExpr = EqualsPredicate(k, other)
    def ===(other: String): RingExpr = EqualsPredicate(k, other)
    def =!=(other: KeyExpr): RingExpr = Not(EqualsPredicate(k,other))
    def =!=(other: String): RingExpr = Not(EqualsPredicate(k, other))
//    def >(other: KeyExpr): RingExpr = IntPredicate(k, other, _ > _, ">")
//    def <(other: KeyExpr): RingExpr = IntPredicate(k, other, _ < _, "<")

    def -->(r: RingExpr): YieldPair = YieldPair(k,r)
  }

  implicit class IffImplicit(pair: (VariableKeyExpr,RingExpr)) {
    def iff(r1: RingExpr): (VariableKeyExpr,(RingExpr,RingExpr)) = (pair._1,(pair._2,r1))
  }

  implicit class VarKeyExprImplicits(x: VariableKeyExpr) {
    def ==>(r: RingExpr): InfiniteMappingExpr = InfiniteMappingExpr(x, r)
  }

  implicit def stringToUnresolvedVarKeyExpr(s: String): UntypedVariableKeyExpr = UntypedVariableKeyExpr(s)

  implicit def intToIntExpr(i: Int): IntExpr = IntExpr(i)
//
  implicit def intToIntKeyExpr(i: Int): PrimitiveKeyExpr[Int] = IntKeyExpr(i)

  implicit def ringExprPairImplicits(p: (RingExpr,RingExpr)): RingProductExpr = RingProductExpr(p._1,p._2)

  implicit def keyExprPairImplicits(p: (KeyExpr,KeyExpr)): KeyProductExpr = KeyProductExpr(p._1,p._2)

  implicit def ringTypePairImplicits(p: (RingType,RingType)): RingType =
    ProductRingType(p._1, p._2)

  implicit def keyTypePairImplicits(p: (KeyType,KeyType)): KeyType =
    ProductKeyType(p._1, p._2)

  implicit def box(r: RingType): KeyType = r.box

  implicit def toK(r: RingExpr): KeyExpr = BoxedRingExpr(r)

  implicit def fromK(k: KeyExpr): RingExpr = FromBoxedRing(k)

  implicit def fromK(s: String): RingExpr = FromBoxedRing(UntypedVariableKeyExpr(s))

  def sng(e: KeyExpr): Sng = Sng(e, IntExpr(1))

  def sng(e: KeyExpr, r: RingExpr) = Sng(e, r)


  case class ForComprehensionBuilder(x: VariableKeyExpr, r1: RingExpr) {
    def Collect(r2: RingExpr): RingExpr = Sum(r1 * {x ==> r2}).inferTypes
    def Yield(pair: YieldPair): RingExpr = Collect(sng(pair.k, pair.r))
    def Yield(k: KeyExpr): RingExpr = Collect(sng(k))
  }

  case class PredicatedForComprehensionBuilder(x: VariableKeyExpr, exprPred: (RingExpr, RingExpr)) {
    val r1 = exprPred._1
    val p = exprPred._2
    val builder = ForComprehensionBuilder(x, r1)
    def Yield(pair: YieldPair): RingExpr =
      builder.Yield(pair.k --> (p dot pair.r))
    def Yield(k: KeyExpr): RingExpr = builder.Yield(k --> p)
  }

  object For {
    def apply(paired: (VariableKeyExpr, RingExpr)): ForComprehensionBuilder =
      ForComprehensionBuilder(paired._1, paired._2)
    def apply(paired: (VariableKeyExpr, (RingExpr,RingExpr))): PredicatedForComprehensionBuilder =
      PredicatedForComprehensionBuilder(paired._1, paired._2)
  }

  case class YieldPair(k: KeyExpr, r: RingExpr)

}
