package slender

sealed trait ShreddedRingExpr {
  def flatExpr: RingExpr
  def ctx: Map[LabelExpr,ShreddedRingExpr]
  //def nestedExpr: RingExpr
}

case class ShreddedLogicalRingExpr(flatExpr: RingExpr, ctx: Map[LabelExpr,ShreddedRingExpr]) extends ShreddedRingExpr

case class ShreddedPhysicalMapping(flatExpr: PhysicalMapping) extends ShreddedRingExpr {
  def ctx: Map[LabelExpr, ShreddedRingExpr] = ???
}
