package slender

trait Expr {

  type Self <: Expr

  //todo - figure out if can make Expr F-bound or use the self type pattern somehow.
  //dont think so - had to make VariablExpr F-bound, which interestingly meant UntypedVariable could not be F-bound,
  //see that code for details.
  def self: Self = this.asInstanceOf[Self]

  def children: List[Expr]

  def eval[T](implicit evaluator: Eval[Self,T]): T = evaluator(self,Map.empty)

  def resolve[T <: Expr](implicit resolver: Resolver[Self,T]): T = resolver(self)

  def shred[Shredded <: Expr](implicit shredder: Shredder[Self,Shredded]): Shredded = shredder(self)

  def shreddable[Shredded <: Expr](implicit canShred: Perhaps[Shredder[Self,Shredded]]) = canShred.value.isDefined

  def evaluable[T](implicit canEval: Perhaps[Eval[Self,_]]) = canEval.value.isDefined

  def id = hashCode.abs.toString.take(3).toInt

  def isResolved: Boolean = children.forall(_.isResolved)

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

trait MyBinaryExpr[E1 <: Expr, E2 <: Expr] extends Expr {
  def c1: E1
  def c2: E2
  def children = List(c1,c2)
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

object Tuple2Expr {
  def apply[R1 <: RingExpr, R2 <: RingExpr](r1: R1, r2: R2): Tuple2RingExpr[R1,R2] = Tuple2RingExpr(r1,r2)
}