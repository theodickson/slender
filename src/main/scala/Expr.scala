package slender

trait Expr {
  type Self <: Expr

  def children: List[Expr]

  def eval[T](implicit evaluator: Eval[Self,T]): T = evaluator(this.asInstanceOf[Self],Map.empty)

  def resolve[T <: Expr](implicit resolver: Resolver[Self,T]): T = resolver(this.asInstanceOf[Self])

  def shred[Shredded <: Expr](implicit shredder: Shredder[Self,Shredded]): Shredded = shredder(this.asInstanceOf[Self])

  def id = hashCode.abs.toString.take(3).toInt


//  def variables: Set[Variable[_]] =
//    children.foldRight(Set.empty[Variable[_]])((v, acc) => acc ++ v.variables)
//  def freeVariables: Set[Variable[_]] =
//    children.foldRight(Set.empty[Variable[_]])((v, acc) => acc ++ v.freeVariables)
  def labels: List[LabelExpr[_]] =
    children.foldLeft(List.empty[LabelExpr[_]])((acc,v) => acc ++ v.labels)
//  def labelDefinitions: Seq[String] = labels.map(_.definition)

//  def brackets: (String,String) = ("","")
//  def openString = toString
//  def closedString = s"${brackets._1}$openString${brackets._2}"

//  def explainVariables: String = "\t" + variables.map(_.explain).mkString("\n\t")
//  def explainLabels: String = labelDefinitions.foldRight("")((v, acc) => acc + "\n" + v)
//  def explain: String = {
//    val border = "-"*80
//    var s = new StringBuilder(s"$border\n")
//    s ++= s"+++ $this +++"
//    s ++= s"\n\nType: $exprType"
//    s ++= s"\n\nVariable types:\n$explainVariables\n"
//    if (isShredded) s ++= s"$explainLabels\n"
//    s ++= border
//    s.mkString
//  }
}

trait C1Expr extends Expr { def c1: Expr }
trait C2Expr extends Expr { def c2: Expr }
trait C3Expr extends Expr { def c3: Expr }

trait NullaryExpr extends Expr {
  def children = List.empty[Expr]
}

trait UnaryExpr extends Expr with C1Expr {
  def children = List(c1)
}

trait BinaryExpr extends Expr with C1Expr with C2Expr {
  def children = List(c1, c2)
}

trait TernaryExpr extends Expr with C1Expr with C2Expr with C3Expr {
  def children = List(c1, c2, c3)
}

trait ProductExpr extends Expr {
  override def toString = s"⟨${children.mkString(",")}⟩"
}

trait Project1Expr extends UnaryExpr {
  def c1: C1Expr
  override def toString = s"$c1._1"
}

trait Project2Expr extends UnaryExpr {
  def c1: C2Expr
  override def toString = s"$c1._2"
}

trait Project3Expr extends UnaryExpr {
  def c1: C3Expr
  override def toString = s"$c1._3"
}

trait PrimitiveExpr[V] extends NullaryExpr {
  def value: V
  override def toString = value.toString
}