package slender

trait Expr {
  def children: Seq[Expr]
  def freeVariables: Set[TypedFreeVariable] =
    children.foldRight(Set.empty[TypedFreeVariable])((v,acc) => acc ++ v.freeVariables)
  def labelExplanations: Seq[String] =
    children.foldRight(Seq.empty[String])((v,acc) => acc ++ v.labelExplanations)
//  def replaceTypes(vars: Map[String,ResolvedKeyType], overwrite: Boolean): this.type
//  def inferTypes(vars: Map[String,ResolvedKeyType]): this.type = replaceTypes(vars, false)
//  def inferTypes: this.type = inferTypes(Map.empty)
}

trait NullaryExpr extends Expr {
  def children = List.empty[Expr]
//  def replaceTypes(vars: Map[String,ResolvedKeyType], overwrite: Boolean): this.type = this
}

trait UnaryExpr extends Expr {
  def c1: Expr
  def children = List(c1)
}

trait BinaryExpr {
  def c1: Expr
  def c2: Expr
  def children = List(c1, c2)
}

trait TernaryExpr {
  def c1: Expr
  def c2: Expr
  def c3: Expr
  def children = List(c1, c2, c3)
}