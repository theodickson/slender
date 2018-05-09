package slender

import scala.collection.mutable.StringBuilder

trait Expr {

  def children: Seq[Expr]

  def exprType: ExprType

  def variables: Set[VariableKeyExpr] =
    children.foldRight(Set.empty[VariableKeyExpr])((v, acc) => acc ++ v.variables)

  def freeVariables: Set[VariableKeyExpr] =
    children.foldRight(Set.empty[VariableKeyExpr])((v, acc) => acc ++ v.freeVariables)

  def labelDefinitions: Seq[String] =
    children.foldLeft(Seq.empty[String])((acc,v) => acc ++ v.labelDefinitions)

  def isShredded: Boolean = children.exists(_.isShredded)

  def isResolved: Boolean = exprType.isResolved

  def isComplete: Boolean = freeVariables.isEmpty

  def explainVariables: String = "\t" + variables.map(_.explain).mkString("\n\t")

  def explainLabels: String = labelDefinitions.foldRight("")((v, acc) => acc + "\n" + v)

  def explain: String = {
    val border = "-"*80
    var s = new StringBuilder(s"$border\n")
    s ++= s"+++ $this +++"
    s ++= s"\n\nType: $exprType"
    s ++= s"\n\nVariable types:\n$explainVariables\n"
    if (isShredded) s ++= s"$explainLabels\n"
    s ++= border
    s.mkString
  }

  def leftBracket = ""
  def rightBracket = ""
  def openString = toString
  def closedString = s"$leftBracket$openString$rightBracket"
}

trait NullaryExpr extends Expr {
  def children = List.empty[Expr]
}

trait UnaryExpr extends Expr {
  def c1: Expr
  def children = List(c1)
}

trait BinaryExpr extends Expr {
  def c1: Expr
  def c2: Expr
  def children = List(c1, c2)
}

trait TernaryExpr extends Expr {
  def c1: Expr
  def c2: Expr
  def c3: Expr
  def children = List(c1, c2, c3)
}