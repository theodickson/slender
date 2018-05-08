package slender

trait Expr {
  def children: Seq[Expr]
  def freeVariables: Set[VariableKeyExpr] =
    children.foldRight(Set.empty[VariableKeyExpr])((v, acc) => acc ++ v.freeVariables)
  def labelExplanations: Seq[String] =
    children.foldLeft(Seq.empty[String])((acc,v) => acc ++ v.labelExplanations)
  def explain: String = s"$this" + "\n\n" + labelExplanations.foldRight("")((v,acc) => acc + "\n" + v)
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