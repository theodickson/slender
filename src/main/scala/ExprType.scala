package slender

trait ExprType {
  def isResolved = true
  def leftBracket = ""
  def rightBracket = ""
  def openString = toString
  def closedString = s"$leftBracket$openString$rightBracket"
}

trait ProductExprType extends ExprType {
  override def leftBracket = "("
  override def rightBracket = ")"
}

trait Tuple2ExprType extends ProductExprType {
  def t1: ExprType
  def t2: ExprType
}

trait Tuple3ExprType extends ProductExprType {
  def t1: ExprType
  def t2: ExprType
  def t3: ExprType
}
