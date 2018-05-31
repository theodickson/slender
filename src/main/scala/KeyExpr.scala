package slender

trait KeyExpr extends Expr {

  type Self <: KeyExpr

  def ===[K1 <: KeyExpr](k1: K1) = EqualsPredicate(self, k1)
  def =!=[K1 <: KeyExpr](k1: K1) = NotExpr(EqualsPredicate(self, k1))

  def >[K1 <: KeyExpr](k1: K1) = IntPredicate(self, k1, _ > _, ">")
  def <[K1 <: KeyExpr](k1: K1) = IntPredicate(self, k1, _ < _, "<")

  def -->[R <: RingExpr](r: R): KeyRingPair[Self,R] = KeyRingPair(self,r)
}

case class KeyRingPair[V <: KeyExpr, R <: RingExpr](k: V, r: R)

trait NullaryKeyExpr extends KeyExpr with NullaryExpr
trait UnaryKeyExpr extends KeyExpr with UnaryExpr
trait BinaryKeyExpr extends KeyExpr with BinaryExpr
trait TernaryKeyExpr extends KeyExpr with TernaryExpr


case class PrimitiveKeyExpr[T](value: T) extends KeyExpr with PrimitiveExpr[T] {
  type Self = PrimitiveKeyExpr[T]
}
object IntKeyExpr {
  def apply(i: Int) = PrimitiveKeyExpr(i)
}
object StringKeyExpr {
  def apply(s: String) = PrimitiveKeyExpr(s)
}


case class Tuple2KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2) extends BinaryKeyExpr with ProductExpr {
  type Self = Tuple2KeyExpr[K1,K2]
}

case class Tuple3KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr, K3 <: KeyExpr](c1: K1, c2: K2, c3: K3)
  extends TernaryKeyExpr with ProductExpr {
  type Self = Tuple3KeyExpr[K1,K2,K3]
}


case class Project1KeyExpr[K <: KeyExpr with C1Expr](c1: K) extends UnaryKeyExpr with Project1Expr {
  type Self = Project1KeyExpr[K]
}

case class Project2KeyExpr[K <: KeyExpr with C2Expr](c1: K) extends UnaryKeyExpr with Project2Expr {
  type Self = Project2KeyExpr[K]
}

case class Project3KeyExpr[K <: KeyExpr with C3Expr](c1: K) extends UnaryKeyExpr with Project3Expr {
  type Self = Project3KeyExpr[K]
}


case class BoxedRingExpr[R <: Expr](c1: R) extends UnaryKeyExpr {
  type Self = BoxedRingExpr[R]
  override def toString = s"[$c1]"
}

case class LabelExpr[R <: RingExpr](c1: R) extends UnaryKeyExpr {

  type Self = LabelExpr[R]

  override def toString = s"Label($id)"

  //  def explainFreeVariables = freeVariables.map(_.explain).mkString("\n\t")
  //
  //  def definition: String = {
  //    val s = new StringBuilder(s"$this: ${c1.exprType}")
  //    s ++= s"\n\n\t$c1"
  //    s ++= s"\n\n\tWhere:\n\t$explainFreeVariables\n"
  //    s.mkString
  //  }
  //  override def labels = c1.labels :+ this
  //
  //  override def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
  //    LabelExpr(c1.replaceTypes(vars, overwrite))

  //  def renest = BoxedRingExpr(FromLabel(LabelExpr(c1.renest))) //todo

}