package slender

trait ExprType {
  def isResolved = true
  def leftBracket = ""
  def rightBracket = ""
  def openString = toString
  def closedString = s"$leftBracket$openString$rightBracket"
}