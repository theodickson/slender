package slender

trait ShreddedRingExpr {
  def flatExpr: RingExpr
  def context: Map[LabelExpr,ShreddedRingExpr]
  def nestedExpr: RingExpr
}