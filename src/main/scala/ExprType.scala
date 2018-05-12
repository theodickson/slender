package slender

trait ExprType {
  def isResolved = true
  def leftBracket = ""
  def rightBracket = ""
  def openString = toString
  def closedString = s"$leftBracket$openString$rightBracket"
}

trait ProductExprType[T <: ExprType] extends ExprType {
  def ts: Seq[T]
  def get(n: Int): T = ts(n-1)
  override def leftBracket = "("
  override def rightBracket = ")"
}