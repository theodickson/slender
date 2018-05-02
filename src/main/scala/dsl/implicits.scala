package slender.dsl

import slender._
import slender.dsl.inference._

object implicits {

  implicit class StringImplicits(s: String) {

    def <--(r: RingExpr): (FreeVariable, RingExpr) = (UntypedFreeVariable(s),r)
    def -->(r: RingExpr): YieldPair = YieldPair(UntypedFreeVariable(s),r)
    //since implicit methods on result of implicit conversions dont tend to resolve,
    //duplicate keyexpr implicits here:
    def _1: KeyExpr = Project1KeyExpr(s)
    def _2: KeyExpr = Project2KeyExpr(s)

    def ===(other: KeyExpr): RingExpr = Predicate(s, other)
    def ===(other: String): RingExpr = Predicate(s, other)
    def =!=(other: KeyExpr): RingExpr = Not(Predicate(s,other))
    def =!=(other: String): RingExpr = Not(Predicate(s,other))

    def ==>(r: RingExpr): InfiniteMappingExpr = InfiniteMappingExpr(s, r)
  }

  implicit class RingExprImplicits(r: RingExpr) {
    def +(r1: RingExpr) = Add(r, r1)
    def *(r1: RingExpr) = Multiply(r, r1)

    def dot(r1: RingExpr) = Dot(r, r1)
    def unary_- = Negate(r)
    def unary_! = Not(r)

    def _1: RingExpr = Project1RingExpr(r)
    def _2: RingExpr = Project2RingExpr(r)

    def && = * _
    def || = this.+ _

    def isTyped: Boolean = r.ringType != UnresolvedRingType

  }

  implicit class KeyExprImplicits(k: KeyExpr) {
    def _1: KeyExpr = Project1KeyExpr(k)
    def _2: KeyExpr = Project2KeyExpr(k)

    def ===(other: KeyExpr): RingExpr = Predicate(k, other)
    def ===(other: String): RingExpr = Predicate(k, other)
    def =!=(other: KeyExpr): RingExpr = Not(Predicate(k,other))
    def =!=(other: String): RingExpr = Not(Predicate(k, other))

    def -->(r: RingExpr): YieldPair = YieldPair(k,r)
  }

  implicit class IffImplicit(pair: (FreeVariable,RingExpr)) {
    def iff(r1: RingExpr): (FreeVariable,(RingExpr,RingExpr)) = (pair._1,(pair._2,r1))
  }

  implicit class VarKeyExprImplicits(x: FreeVariable) {
    def ==>(r: RingExpr): InfiniteMappingExpr = InfiniteMappingExpr(x, r)
  }

  implicit def stringToUnresolvedVarKeyExpr(s: String): UntypedFreeVariable = UntypedFreeVariable(s)

  implicit def intToIntExpr(i: Int): IntExpr = IntExpr(i)

  implicit def intToIntKeyExpr(i: Int): IntKeyExpr = IntKeyExpr(i)

  implicit def ringExprPairImplicits(p: (RingExpr,RingExpr)): RingPairExpr = RingPairExpr(p._1,p._2)

  implicit def keyExprPairImplicits(p: (KeyExpr,KeyExpr)): KeyPairExpr = KeyPairExpr(p._1,p._2)

  implicit def ringTypePairImplicits(p: (ResolvedRingType,ResolvedRingType)): ResolvedRingType =
    RingPairType(p._1, p._2)

  implicit def keyTypePairImplicits(p: (ResolvedKeyType,ResolvedKeyType)): ResolvedKeyType =
    KeyPairType(p._1, p._2)

  implicit def box(r: ResolvedRingType): KeyType = r.box

  implicit def toK(r: RingExpr): KeyExpr = BoxedRingExpr(r)

  implicit def fromK(k: BoxedRingExpr): RingExpr = k.r

  implicit def fromK(s: String): RingExpr = FromBoxedRing(UntypedFreeVariable(s))

  def sng(e: KeyExpr): Sng = Sng(e, IntExpr(1))

  def sng(e: KeyExpr, r: RingExpr) = Sng(e, r)


  case class ForComprehensionBuilder(x: FreeVariable, r1: RingExpr) {
    def Collect(r2: RingExpr): RingExpr = inferTypes(Sum(r1 * {x ==> r2}))
    def Yield(pair: YieldPair): RingExpr = Collect(sng(pair.k, pair.r))
    def Yield(k: KeyExpr): RingExpr = Collect(sng(k))
  }

  case class PredicatedForComprehensionBuilder(x: FreeVariable, exprPred: (RingExpr, RingExpr)) {
    val r1 = exprPred._1
    val p = exprPred._2
    val builder = ForComprehensionBuilder(x, r1)
    def Yield(pair: YieldPair): RingExpr =
      builder.Yield(pair.k --> (p dot pair.r))
    def Yield(k: KeyExpr): RingExpr = builder.Yield(k --> p)
  }

  object For {
    def apply(paired: (FreeVariable, RingExpr)): ForComprehensionBuilder =
      ForComprehensionBuilder(paired._1, paired._2)
    def apply(paired: (FreeVariable, (RingExpr,RingExpr))): PredicatedForComprehensionBuilder =
      PredicatedForComprehensionBuilder(paired._1, paired._2)
  }

  case class YieldPair(k: KeyExpr, r: RingExpr)

}
