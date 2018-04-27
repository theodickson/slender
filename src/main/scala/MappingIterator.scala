package slender

import dsl._

case class ForComprehensionBuilder(x: VarKeyExpr, r1: MappingRingExpr) {
  def Collect(r2: RingExpr): RingExpr = Sum(r1 * {x ==> r2})
  def Yield(pair: (KeyExpr,RingExpr)): RingExpr = Collect(sng(pair._1, pair._2))
  def Yield(k: KeyExpr): RingExpr = Collect(sng(k))
}

case class PredicatedForComprehensionBuilder(x: VarKeyExpr, exprPred: (MappingRingExpr,Predicate)) {

  val r1 = exprPred._1
  val p = exprPred._2
  val builder = ForComprehensionBuilder(x, r1)

  def Yield(keyRing: (KeyExpr,RingExpr)): RingExpr = {
    val keyExpr = keyRing._1
    val ringExpr = keyRing._2
    builder.Yield((keyExpr, p dot ringExpr))
  }

  def Yield(k: KeyExpr): RingExpr = builder.Yield((k, p))
}

object For {
  def apply(paired: (VarKeyExpr,MappingRingExpr)): ForComprehensionBuilder =
    ForComprehensionBuilder(paired._1, paired._2)
}

case class PredicatedMappingRingExpr(m: MappingRingExpr, p: Predicate)
