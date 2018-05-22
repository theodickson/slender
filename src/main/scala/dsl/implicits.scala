package slender.dsl

import slender._
import scala.reflect.runtime.universe._

object implicits {

  implicit class KeyExprOps(val k: KeyExpr) {
    def _1: KeyExpr = ProjectKeyExpr(k, 1)
    def _2: KeyExpr = ProjectKeyExpr(k, 2)
    def _3: KeyExpr = ProjectKeyExpr(k, 3)

    def ===(other: KeyExpr): RingExpr = EqualsPredicate(k, other)
    def =!=(other: KeyExpr): RingExpr = Not(EqualsPredicate(k,other))

    def >(other: KeyExpr): RingExpr = IntPredicate(k, other, _ > _, ">")
    def <(other: KeyExpr): RingExpr = IntPredicate(k, other, _ < _, "<")

    def -->(r: RingExpr): YieldPair = YieldPair(k,r)
  }

  implicit class VarKeyExprOps(val k: VariableKeyExpr) {
    def <--(r: RingExpr): (VariableKeyExpr, RingExpr) = (k,r)
    def ==>(r: RingExpr): InfiniteMappingExpr = InfiniteMappingExpr(k, r)
  }

  trait MakeKeyExpr[X] extends (X => KeyExpr)
  trait MakeKeyExprOps[X] extends (X => KeyExprOps)
  trait MakeVarKeyExprOps[X] extends (X => VarKeyExprOps)

  implicit def toKeyExpr[X](x: X)(implicit make: MakeKeyExpr[X]): KeyExpr = make(x)
  implicit def toKeyExprOps[X](x: X)(implicit make: MakeKeyExprOps[X]): KeyExprOps = make(x)
  implicit def toVarKeyExprOps[X](x: X)(implicit make: MakeVarKeyExprOps[X]): VarKeyExprOps = make(x)


  implicit object idMakeKeyExpr extends MakeKeyExpr[KeyExpr] {
    def apply(x: KeyExpr) = x
  }

  implicit object stringMakeKeyExpr extends MakeKeyExpr[String] {
    def apply(x: String) = UntypedVariable(x)
  }

  implicit object idMakeKeyExprOps extends MakeKeyExprOps[KeyExpr] {
    def apply(x: KeyExpr) = KeyExprOps(x)
  }

  implicit object stringMakeKeyExprOps extends MakeKeyExprOps[String] {
    def apply(x: String) = KeyExprOps(UntypedVariable(x))
  }

  implicit object idMakeVarKeyExprOps extends MakeVarKeyExprOps[VariableKeyExpr] {
    def apply(x: VariableKeyExpr) = VarKeyExprOps(x)
  }

  implicit object stringMakeVarKeyExprOps extends MakeVarKeyExprOps[String] {
    def apply(x: String) = VarKeyExprOps(UntypedVariable(x))
  }

  implicit def pairMakeKeyExpr[A,B](implicit recur1: MakeKeyExpr[A],
                                             recur2: MakeKeyExpr[B]): MakeKeyExpr[(A,B)]
  = new MakeKeyExpr[(A,B)] {
    def apply(x: (A,B)) =
      KeyProductExpr(
        List[KeyExpr](recur1(x._1), recur2(x._2))
      )
  }

  implicit def pairMakeKeyExprOps[A,B](implicit recur1: MakeKeyExprOps[A],
                                       recur2: MakeKeyExprOps[B]): MakeKeyExprOps[(A,B)]
  = new MakeKeyExprOps[(A,B)] {
    def apply(x: (A,B)) = KeyExprOps(
      KeyProductExpr(
        List[KeyExpr](recur1(x._1).k, recur2(x._2).k)
      )
    )
  }

  implicit def pairMakeVarKeyExprOps[A,B](implicit recur1: MakeVarKeyExprOps[A],
                                       recur2: MakeVarKeyExprOps[B]): MakeVarKeyExprOps[(A,B)]
    = new MakeVarKeyExprOps[(A,B)] {
    def apply(x: (A,B)) = VarKeyExprOps(
      ProductVariableKeyExpr(
        List[VariableKeyExpr](recur1(x._1).k, recur2(x._2).k)
      )
    )
  }

  implicit def tripleMakeKeyExpr[A,B,C](implicit recur1: MakeKeyExpr[A],
                                    recur2: MakeKeyExpr[B], recur3: MakeKeyExpr[C]): MakeKeyExpr[(A,B,C)]
  = new MakeKeyExpr[(A,B,C)] {
    def apply(x: (A,B,C)) =
      KeyProductExpr(
        List[KeyExpr](recur1(x._1), recur2(x._2), recur3(x._3))
      )
  }

  implicit def tripleMakeKeyExprOps[A,B,C](implicit recur1: MakeKeyExprOps[A],
                                       recur2: MakeKeyExprOps[B], recur3: MakeKeyExprOps[C]): MakeKeyExprOps[(A,B,C)]
  = new MakeKeyExprOps[(A,B,C)] {
    def apply(x: (A,B,C)) = KeyExprOps(
      KeyProductExpr(
        List[KeyExpr](recur1(x._1).k, recur2(x._2).k, recur3(x._3).k)
      )
    )
  }

  implicit def tripleMakeVarKeyExprOps[A,B,C](implicit recur1: MakeVarKeyExprOps[A],
                                          recur2: MakeVarKeyExprOps[B], recur3: MakeVarKeyExprOps[C]): MakeVarKeyExprOps[(A,B,C)]
  = new MakeVarKeyExprOps[(A,B,C)] {
    def apply(x: (A,B,C)) = VarKeyExprOps(
      ProductVariableKeyExpr(
        List[VariableKeyExpr](recur1(x._1).k, recur2(x._2).k, recur3(x._3).k)
      )
    )
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

  implicit class IffImplicit(pair: (VariableKeyExpr,RingExpr)) {
    def iff(r1: RingExpr): (VariableKeyExpr,(RingExpr,RingExpr)) = (pair._1,(pair._2,r1))
  }

  implicit def intToIntExpr(i: Int): IntExpr = IntExpr(i)

  implicit def intToIntKeyExpr(i: Int): PrimitiveKeyExpr[Int] = IntKeyExpr(i)

  implicit def ringExprPairImplicits(p: (RingExpr,RingExpr)): RingProductExpr = RingProductExpr(p._1,p._2)

  implicit def keyExprPairImplicits(p: (KeyExpr,KeyExpr)): KeyProductExpr = KeyProductExpr(p._1,p._2)

  implicit def ringTypePairImplicits(p: (RingType,RingType)): RingType =
    ProductRingType(p._1, p._2)

  implicit def keyTypePairImplicits(p: (KeyType,KeyType)): KeyType =
    ProductKeyType(p._1, p._2)


  implicit def toK(r: RingExpr): KeyExpr = BoxedRingExpr(r)

  implicit def fromK(k: KeyExpr): RingExpr = FromBoxedRing(k)

  implicit def fromK(s: String): RingExpr = FromBoxedRing(UntypedVariable(s))

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


  /**IntelliJ is not managing to resolve the conversions via the type-class mediator pattern even though its compiling
    * so here are a bunch of explicit conversions to stop IDE errors. Hopefully can sort this out at some point.
    * At time of writing everything compiles and tests run successfully with the below commented out so should
    * periodically check that's still the case!
    */


  implicit def stringPairToVarKeyExpr(p: (String,String)): VariableKeyExpr =
    ProductVariableKeyExpr(List(p._1, p._2))

  implicit def stringPairToVarKeyExprOps(p: (String,String)): VarKeyExprOps =
    VarKeyExprOps(ProductVariableKeyExpr(List(p._1, p._2)))

  implicit def stringTripleToVarKeyExprOps(p: (String,String,String)): VarKeyExprOps =
    VarKeyExprOps(ProductVariableKeyExpr(List(p._1, p._2, p._3)))

  implicit def oddPair1(p: (String,KeyExpr)): KeyProductExpr =
    KeyProductExpr(List(UntypedVariable(p._1),p._2))

  implicit def oddPair2(p: (KeyExpr,String)): KeyProductExpr =
    KeyProductExpr(List(p._1,UntypedVariable(p._2)))

}
