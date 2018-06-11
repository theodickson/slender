package slender

import scala.reflect.runtime.universe._

sealed trait Expr {

  def children: List[Expr]

  def id = hashCode.abs.toString.take(3).toInt

  def isResolved: Boolean = children.forall(_.isResolved)
//  def variables: Set[Variable[_]] =
//    children.foldRight(Set.empty[Variable[_]])((v, acc) => acc ++ v.variables)
//  def freeVariables: Set[Variable[_]] =
//    children.foldRight(Set.empty[Variable[_]])((v, acc) => acc ++ v.freeVariables)
//  def labels: List[LabelExpr[_]] =
//    children.foldLeft(List.empty[LabelExpr[_]])((acc,v) => acc ++ v.labels)
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

sealed trait C1Expr extends Expr { def c1: Expr }
sealed trait C2Expr extends Expr { def c2: Expr }
sealed trait C3Expr extends Expr { def c3: Expr }

sealed trait NullaryExpr extends Expr {
  def children = List.empty[Expr]
}

sealed trait UnaryExpr extends Expr with C1Expr {
  def children = List(c1)
}

sealed trait BinaryExpr extends Expr with C1Expr with C2Expr {
  def children = List(c1, c2)
}

sealed trait TernaryExpr extends Expr with C1Expr with C2Expr with C3Expr {
  def children = List(c1, c2, c3)
}

sealed trait ProductExpr extends Expr {
  override def toString = s"⟨${children.mkString(",")}⟩"
}

sealed trait Project1Expr extends UnaryExpr {
  def c1: Expr with C1Expr
  override def toString = s"$c1._1"
}

sealed trait Project2Expr extends UnaryExpr {
  def c1: Expr with C2Expr
  override def toString = s"$c1._2"
}

sealed trait Project3Expr extends UnaryExpr {
  def c1: Expr with C3Expr
  override def toString = s"$c1._3"
}

sealed trait PrimitiveExpr[V] extends NullaryExpr {
  def value: V
  override def toString = value.toString
}




/**KeyExpr*/
sealed trait KeyExpr extends Expr

sealed trait NullaryKeyExpr extends KeyExpr with NullaryExpr
sealed trait UnaryKeyExpr extends KeyExpr with UnaryExpr
sealed trait BinaryKeyExpr extends KeyExpr with BinaryExpr
sealed trait TernaryKeyExpr extends KeyExpr with TernaryExpr

case class PrimitiveKeyExpr[T](value: T) extends KeyExpr with PrimitiveExpr[T]

object IntKeyExpr {
  def apply(i: Int) = PrimitiveKeyExpr(i)
}
object StringKeyExpr {
  def apply(s: String) = PrimitiveKeyExpr(s)
}

case class Tuple2KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2) extends BinaryKeyExpr with ProductExpr

case class Tuple3KeyExpr[K1 <: KeyExpr, K2 <: KeyExpr, K3 <: KeyExpr](c1: K1, c2: K2, c3: K3)
  extends TernaryKeyExpr with ProductExpr

case class Project1KeyExpr[K <: KeyExpr with C1Expr](c1: K) extends UnaryKeyExpr with Project1Expr

case class Project2KeyExpr[K <: KeyExpr with C2Expr](c1: K) extends UnaryKeyExpr with Project2Expr

case class Project3KeyExpr[K <: KeyExpr with C3Expr](c1: K) extends UnaryKeyExpr with Project3Expr

case class BoxedRingExpr[R <: Expr](c1: R) extends UnaryKeyExpr {
  override def toString = s"[$c1]"
}

case class LabelExpr[R <: RingExpr](c1: R) extends UnaryKeyExpr {
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





/**RingExpr*/
sealed trait RingExpr extends Expr

sealed trait NullaryRingExpr extends RingExpr with NullaryExpr
sealed trait UnaryRingExpr extends RingExpr with UnaryExpr
sealed trait BinaryRingExpr extends RingExpr with BinaryExpr
sealed trait TernaryRingExpr extends RingExpr with TernaryExpr

/**Primitive ring expressions*/
sealed trait PrimitiveRingExpr[T] extends RingExpr with PrimitiveExpr[T]

case class NumericExpr[N : Numeric](value: N) extends PrimitiveRingExpr[N]

case class PhysicalCollection[C[_,_],K,R](value: C[K,R])(implicit collection: Collection[C,K,R])
  extends PrimitiveRingExpr[C[K,R]]

object PhysicalCollection {
  def apply[T](value: Set[T])(implicit collection: Collection[Map,T,Int]): PhysicalCollection[Map,T,Int] =
    PhysicalCollection(value.map((_,1)).toMap)
}

/**Standard ring operations*/
sealed trait BinaryRingOpExpr extends BinaryRingExpr {
  def opString: String
  //  final def brackets = ("(",")")
  //  override def toString = s"${c1.closedString} $opString ${c2.closedString}"
}

case class AddExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {
  def opString = "+"
}

case class MultiplyExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {
  def opString = "*"
}

case class DotExpr[E1 <: RingExpr,E2 <: RingExpr](c1: E1, c2: E2)
  extends BinaryRingOpExpr {
  def opString = "⊙"
}

case class NotExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

case class NegateExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

case class SumExpr[E <: RingExpr](c1: E) extends UnaryRingExpr

/**Mapping constructs*/
case class InfiniteMappingExpr[K <: VariableExpr[K],R <: RingExpr](key: K, value: R)
  extends BinaryRingExpr {
  def c1 = key; def c2 = value
  override def toString = s"{$key => $value}"
}

case class SngExpr[K <: KeyExpr,R <: RingExpr](key: K, value: R)
  extends BinaryRingExpr {
  def c1 = key; def c2 = value
}

/**Predicates*/
sealed trait Predicate extends BinaryRingExpr {
  def opString: String
  //  override def brackets = ("(",")")
  override def toString = s"$c1 $opString $c2"
}

case class EqualsPredicate[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2) extends Predicate {
  def opString = "="
}

case class IntPredicate[K1 <: KeyExpr, K2 <: KeyExpr](c1: K1, c2: K2, p: (Int,Int) => Boolean, opString: String)
  extends Predicate

object Predicate {
  def apply[K1 <: KeyExpr, K2 <: KeyExpr](k1: K1, k2: K2) = EqualsPredicate(k1,k2)
}

/**Conversions from keys*/
////sealed trait FromK extends RingExpr with UnaryExpr[E[KeyExpr]
////
////object FromK {
////  def apply(k: KeyExpr): FromK = k.exprType match {
////    case BoxedRingType(_) => FromBoxedRing(k)
////    case LabelType(_) => FromLabel(k)
////    case t => throw InvalidFromKException(s"Cannot create ring expr from key expr with type $t")
////  }
////}
////
case class ToRingExpr[E <: Expr](c1: E) extends UnaryRingExpr
//case class FromBoxedRing[R,E <: Expr[R,E]](c1: BoxedRingExpr[R,E]) extends Expr[R,FromBoxedRing[R,E]] with UnaryExpr[R,BoxedRingExpr[R,E]] {
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) =
////    FromBoxedRing(c1.replaceTypes(vars, overwrite))
////
//  def _eval(vars: BoundVars): R = c1._eval(vars)
//}
//
//
//case class FromLabel[O,E <: Expr[O,E]](c1: LabelExpr[O,E]) extends Expr[O,FromLabel[O,E]] with UnaryExpr[Label[O,E],LabelExpr[O,E]] {
////
////  def replaceTypes(vars: Map[String, KeyType], overwrite: Boolean) = FromLabel(c1.replaceTypes(vars, overwrite))
//
//  def _eval(vars: BoundVars): O = c1._eval(vars).eval //todo - do I actually need to use the free variables stored in the label?
//  def shred = ???
////  def renest = FromK(c1.renest)
//}


/**Tupling and projection*/
//case class ProductRingExpr[L <: HList, Lub <: LUBConstraint[L, RingExpr]]
//  (l: L)(implicit lub: Lub, trav: ToTraversable.Aux[L, List, Lub] { type Out <: List[RingExpr] }) extends RingExpr with ProductExpr {
//  type Self = ProductRingExpr[L, Lub]
//  def children: List[RingExpr] = l.toList[Lub](trav)
//}

//case class ProductRingExpr[L <: HList](l: L)
//                                      (implicit lub: LUBConstraint[L,RingExpr],
//                                                toList: ToList[L, RingExpr])
//  extends RingExpr with ProductExpr {
//  type Self = ProductRingExpr[L]
//  def children: List[RingExpr] = l.toList//.asInstanceOf[List[RingExpr]]//l.toList(trav).map(_.asInstanceOf[RingExpr])//l.toList[Lub](trav)
//}

case class Tuple2RingExpr[K1 <: RingExpr, K2 <: RingExpr](c1: K1, c2: K2) extends BinaryRingExpr with ProductExpr

case class Tuple3RingExpr[K1 <: RingExpr, K2 <: RingExpr, K3 <: RingExpr](c1: K1, c2: K2, c3: K3)
  extends TernaryRingExpr with ProductExpr

case class Project1RingExpr[K <: RingExpr with C1Expr](c1: K) extends UnaryRingExpr with Project1Expr

case class Project2RingExpr[K <: RingExpr with C2Expr](c1: K) extends UnaryRingExpr with Project2Expr

case class Project3RingExpr[K <: RingExpr with C3Expr](c1: K) extends UnaryRingExpr with Project3Expr

sealed trait VariableExpr[V <: VariableExpr[V]] extends KeyExpr {
  type Type
  def bind(t: Type): BoundVars
}

case class TypedVariable[T](name: String) extends VariableExpr[TypedVariable[T]] with NullaryKeyExpr {
  type Type = T
  override def toString = name
  def bind(t: T) = Map(this.name -> t)
  override def isResolved = true
}


sealed trait UntypedVariable[T <: VariableExpr[T]] extends VariableExpr[T] with NullaryKeyExpr {
  type Type = Untyped
  def name: String
  def tag[KT]: TypedVariable[KT] = TypedVariable[KT](name)
  def bind(t: Untyped) = ???
  override def toString = s""""$this:?""""
  override def isResolved = false
}

case class Tuple2VariableExpr[V1 <: VariableExpr[V1],V2 <: VariableExpr[V2]](c1: V1, c2: V2)
  extends VariableExpr[Tuple2VariableExpr[V1,V2]] with BinaryExpr with ProductExpr {
  type Type = (c1.Type,c2.Type)
  def bind(t: (c1.Type,c2.Type)) = c1.bind(t._1) ++ c2.bind(t._2)
}

case class Tuple3VariableExpr[V1 <: VariableExpr[V1],V2 <: VariableExpr[V2],V3 <: VariableExpr[V3]](c1: V1, c2: V2, c3: V3)
  extends VariableExpr[Tuple3VariableExpr[V1,V2,V3]] with BinaryExpr with ProductExpr {
  type Type = (c1.Type,c2.Type,c3.Type)
  def bind(t: (c1.Type,c2.Type,c3.Type)) = c1.bind(t._1) ++ c2.bind(t._2) ++ c3.bind(t._3)
}

sealed trait _C extends UntypedVariable[_C] { override def name = "C" }
sealed trait _C1 extends UntypedVariable[_C1] { override def name = "C1" }
sealed trait _C2 extends UntypedVariable[_C2] { override def name = "C2" }
sealed trait _C3 extends UntypedVariable[_C3] { override def name = "C3" }

sealed trait _O extends UntypedVariable[_O] { override def name = "O" }
sealed trait _O1 extends UntypedVariable[_O1] { override def name = "O1" }
sealed trait _O2 extends UntypedVariable[_O2] { override def name = "O2" }
sealed trait _O3 extends UntypedVariable[_O3] { override def name = "O3" }

sealed trait _P extends UntypedVariable[_P] { override def name = "P" }
sealed trait _P1 extends UntypedVariable[_P1] { override def name = "P1" }
sealed trait _P2 extends UntypedVariable[_P2] { override def name = "P2" }
sealed trait _P3 extends UntypedVariable[_P3] { override def name = "P3" }

sealed trait _S extends UntypedVariable[_S] { override def name = "S" }
sealed trait _S1 extends UntypedVariable[_S1] { override def name = "S1" }
sealed trait _S2 extends UntypedVariable[_S2] { override def name = "S2" }
sealed trait _S3 extends UntypedVariable[_S3] { override def name = "S3" }

sealed trait _W extends UntypedVariable[_W] { override def name = "W" }
sealed trait _W1 extends UntypedVariable[_W1] { override def name = "W1" }
sealed trait _W2 extends UntypedVariable[_W2] { override def name = "W2" }
sealed trait _W3 extends UntypedVariable[_W3] { override def name = "W3" }

sealed trait _X extends UntypedVariable[_X] { override def name = "X" }
sealed trait _X1 extends UntypedVariable[_X1] { override def name = "X1" }
sealed trait _X2 extends UntypedVariable[_X2] { override def name = "X2" }
sealed trait _X3 extends UntypedVariable[_X3] { override def name = "X3" }

sealed trait _Y extends UntypedVariable[_Y] { override def name = "Y" }
sealed trait _Y1 extends UntypedVariable[_Y1] { override def name = "Y1" }
sealed trait _Y2 extends UntypedVariable[_Y2] { override def name = "Y2" }
sealed trait _Y3 extends UntypedVariable[_Y3] { override def name = "Y3" }

sealed trait _Z extends UntypedVariable[_Z] { override def name = "Z" }
sealed trait _Z1 extends UntypedVariable[_Z1] { override def name = "Z1" }
sealed trait _Z2 extends UntypedVariable[_Z2] { override def name = "Z2" }
sealed trait _Z3 extends UntypedVariable[_Z3] { override def name = "Z3" }

sealed trait _K extends UntypedVariable[_K] { override def name = "K" }
sealed trait _K1 extends UntypedVariable[_K1] { override def name = "K1" }
sealed trait _K2 extends UntypedVariable[_K2] { override def name = "K2" }
sealed trait _K3 extends UntypedVariable[_K3] { override def name = "K3" }


trait Variables {

  val C = new _C {}
  val C1 = new _C1 {}
  val C2 = new _C2 {}
  val C3 = new _C3 {}

  val O = new _O {}
  val O1 = new _O1 {}
  val O2 = new _O2 {}
  val O3 = new _O3 {}

  val P = new _P {}
  val P1 = new _P1 {}
  val P2 = new _P2 {}
  val P3 = new _P3 {}

  val S = new _S {}
  val S1 = new _S1 {}
  val S2 = new _S2 {}
  val S3 = new _S3 {}

  val X = new _X {}
  val X1 = new _X1 {}
  val X2 = new _X2 {}
  val X3 = new _X3 {}

  val Y = new _Y {}
  val Y1 = new _Y1 {}
  val Y2 = new _Y2 {}
  val Y3 = new _Y3 {}

  val Z = new _Z {}
  val Z1 = new _Z1 {}
  val Z2 = new _Z2 {}
  val Z3 = new _Z3 {}

  val W = new _W {}
  val W1 = new _W1 {}
  val W2 = new _W2 {}
  val W3 = new _W3 {}

  val K = new _K {}
  val K1 = new _K1 {}
  val K2 = new _K2 {}
  val K3 = new _K3 {}

}



trait ExprImplicits {

  implicit class ExprOps[E <: Expr](e: E) {

    def eval[R <: Expr,T](implicit resolve: Resolver[E,R], evaluator: Eval[R,T]): T = evaluator(resolve(e),Map.empty)

    def evalType[T : TypeTag, R <: Expr](implicit resolve: Resolver[E,R], evaluator: Eval[R,T]): Type = typeTag[T].tpe

    def resolve[T <: Expr](implicit resolver: Resolver[E,T]): T = resolver(e)

    def shred[Shredded <: Expr](implicit shredder: Shredder[E,Shredded]): Shredded = shredder(e)

    def shreddable[Shredded <: Expr](implicit canShred: Perhaps[Shredder[E,Shredded]]) = canShred.value.isDefined

    def isEvaluable[T](implicit canEval: Perhaps[Eval[E,_]]) = canEval.value.isDefined

    def isResolvable[T](implicit canResolve: Perhaps[Resolver[E,_]]): Boolean = canResolve.value.isDefined
  }

  implicit class KeyExprOps[K <: KeyExpr](k: K) {
    def ===[K1 <: KeyExpr](k1: K1) = EqualsPredicate(k, k1)
    def =!=[K1 <: KeyExpr](k1: K1) = NotExpr(EqualsPredicate(k, k1))

    def >[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ > _, ">")
    def <[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ < _, "<")

    def -->[R <: RingExpr](r: R): KeyRingPair[K,R] = KeyRingPair(k,r)
  }

  implicit class RingExprOps[R <: RingExpr](r: R) {
    def +[R1 <: RingExpr](expr1: R1) = AddExpr(r,expr1)

    def *[R1 <: RingExpr](expr1: R1) = MultiplyExpr(r,expr1)

    def dot[R1 <: RingExpr](expr1: R1) = DotExpr(r,expr1)

    def sum = SumExpr(r)

    def unary_- = NegateExpr(r)

    def unary_! = NotExpr(r)

    def &&[R1 <: RingExpr](expr1: R1) = this.*[R1](expr1)

    def ||[R1 <: RingExpr](expr1: R1) = this.+[R1](expr1)
  }

  implicit class VariableExprOps[V <: VariableExpr[V]](v: V) {
    def <--[R <: RingExpr](r: R): VariableRingPredicate[V,R,NumericExpr[Int]] = VariableRingPredicate(v,r)
    def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[V,R] = InfiniteMappingExpr(v,r)
  }


}
