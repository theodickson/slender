package slender

import org.apache.spark.rdd.RDD
import shapeless.ops.hlist.ToTraversable
import shapeless._
import shapeless.syntax._

import scala.reflect.runtime.universe._

case class VariableRingPredicate[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](k: V, r: R, p: P = NumericExpr(1))
case class KeyRingPair[V <: KeyExpr, R <: RingExpr](k: V, r: R)

case class ExprOps[E <: Expr](e: E) {
  def eval[R <: Expr,T](implicit resolve: Resolver[E,R], evaluator: Eval[R,T]): T = evaluator(resolve(e),Map.empty)
  def evalType[T : TypeTag, R <: Expr](implicit resolve: Resolver[E,R], evaluator: Eval[R,T]): Type = typeTag[T].tpe
  def resolve[T <: Expr](implicit resolver: Resolver[E,T]): T = resolver(e)
  def shred[Shredded <: Expr](implicit shredder: Shredder[E,Shredded]): Shredded = shredder(e)
  def shreddable[Shredded <: Expr](implicit canShred: Perhaps[Shredder[E,Shredded]]) = canShred.value.isDefined
  def isEvaluable[T](implicit canEval: Perhaps[Eval[E,_]]) = canEval.value.isDefined
  def isResolvable[T](implicit canResolve: Perhaps[Resolver[E,_]]): Boolean = canResolve.value.isDefined
}

case class KeyExprOps[K <: KeyExpr](k: K) {
  def ===[K1 <: KeyExpr](k1: K1) = EqualsPredicate(k, k1)
  def =!=[K1 <: KeyExpr](k1: K1) = NotExpr(EqualsPredicate(k, k1))
  def >[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ > _, ">")
  def <[K1 <: KeyExpr](k1: K1) = IntPredicate(k, k1, _ < _, "<")
  def -->[R <: RingExpr](r: R): KeyRingPair[K,R] = KeyRingPair(k,r)
}

case class RingExprOps[R <: RingExpr](r: R) {
  def +[R1 <: RingExpr](expr1: R1) = AddExpr(r,expr1)
  def *[R1 <: RingExpr](expr1: R1) = MultiplyExpr(r,expr1)
  def dot[R1 <: RingExpr](expr1: R1) = DotExpr(r,expr1)
  def join[R1 <: RingExpr](expr1: R1) = JoinExpr(r,expr1)
  def sum = SumExpr(r)
  def unary_- = NegateExpr(r)
  def unary_! = NotExpr(r)
  def &&[R1 <: RingExpr](expr1: R1) = this.*[R1](expr1)
  def ||[R1 <: RingExpr](expr1: R1) = this.+[R1](expr1)
}

case class VariableExprOps[V <: VariableExpr[V]](v: V) {
  def <--[R <: RingExpr](r: R): VariableRingPredicate[V,R,NumericExpr[Int]] = VariableRingPredicate(v,r)
  def ==>[R <: RingExpr](r: R): InfiniteMappingExpr[V,R] = InfiniteMappingExpr(v,r)
}

case class ForComprehensionBuilder[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](vrp: VariableRingPredicate[V,R,P]) {
  //todo - get rid of default NumericExpr(1)

  val x = vrp.k; val r1 = vrp.r; val p = vrp.p

  def _collect[R2 <: RingExpr](r2: R2): SumExpr[MultiplyExpr[R, InfiniteMappingExpr[V, DotExpr[R2,P]]]] =
    SumExpr(MultiplyExpr(r1, InfiniteMappingExpr(x, DotExpr(r2,p))))

  def Collect[T, R2 <: RingExpr]
  (r2: T)
  (implicit make: MakeExpr[T,R2]) = _collect(make(r2))

  def Yield[T, K <: KeyExpr, R2 <: RingExpr]
  (x: T)
  (implicit make: MakeKeyRingPair[T,K,R2]) = {
    val made = make(x)
    _collect(SngExpr(made.k, made.r))
  }
}

case class NestedForComprehensionBuilder[
V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], R1 <: RingExpr, R2 <: RingExpr, P1 <: RingExpr, P2 <: RingExpr
](builder1: ForComprehensionBuilder[V1,R1,P1], builder2: ForComprehensionBuilder[V2,R2,P2]) {
  //todo - this works but takes forever to compile. Perhaps a more elegant and general solution might be quicker,
  //otherwise can it.
  def Collect[T, R3 <: RingExpr]
  (r3: T)(implicit make: MakeExpr[T, R3]) = {
    val made = make(r3)
    builder1._collect(builder2._collect(made))
  }

  def Yield[T, K <: KeyExpr, R3 <: RingExpr]
  (r3: T)(implicit make: MakeKeyRingPair[T, K, R3]) = {
    val made = make(r3)
    builder1._collect(builder2._collect(SngExpr(made.k, made.r)))
  }
}

trait MakeExpr[X,E <: Expr] extends (X => E)
trait HMakeExpr[X,E] extends (X => E)

object MakeExpr extends Priority2MakeExprImplicits {
  def instance[X,E <: Expr](f: X => E): MakeExpr[X,E] = new MakeExpr[X,E] {
    def apply(v1: X): E = f(v1)
  }
}

object HMakeExpr {

  implicit def hnilMakeExpr: HMakeExpr[HNil, HNil] = new HMakeExpr[HNil,HNil] {
    def apply(v1: HNil): HNil = v1
  }

  implicit def hconsMakeExpr[H, HE <: Expr, T <: HList, TE <: HList](implicit makeH: MakeExpr[H,HE], makeT: HMakeExpr[T,TE]):
  HMakeExpr[H::T,HE::TE] = new HMakeExpr[H::T,HE::TE] {
    def apply(v1: H::T) = v1 match {
      case h :: t => makeH(h) :: makeT(t)
    }
  }

}

trait Priority0MakeExprImplicits {

  implicit def toKMakeExpr[T,R <: RingExpr](implicit recur: MakeExpr[T,R]): MakeExpr[T,BoxedRingExpr[R]] =
    new MakeExpr[T,BoxedRingExpr[R]] {
      def apply(v1: T) = BoxedRingExpr(recur(v1))
    }
}

trait Priority1MakeExprImplicits extends Priority0MakeExprImplicits {

  implicit def idMakeExpr[E <: Expr]: MakeExpr[E,E] = new MakeExpr[E,E] { def apply(v1: E) = v1 }

  implicit def numericMakeRing[N : Numeric]: MakeExpr[N,NumericExpr[N]] = MakeExpr.instance { v1: N => NumericExpr(v1) }

//  implicit def collectionMakeRing[C[_,_],K,R](implicit coll: Collection[C,K,R]): MakeExpr[C[K,R],PhysicalCollection[C,K,R]] =
//      MakeExpr.instance { mp: C[K,R] => PhysicalCollection(mp) }

//  implicit def tuple2MakeRingExpr[X1,X2,R1 <: RingExpr, R2 <: RingExpr]
//  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2RingExpr[R1,R2]] =
//    new MakeExpr[(X1,X2),Tuple2RingExpr[R1,R2]] {
//      def apply(v1: (X1,X2)) = Tuple2RingExpr(recur1(v1._1),recur2(v1._2))
//    }
//
//  implicit def tuple3MakeRingExpr[X1,X2,X3,R1 <: RingExpr, R2 <: RingExpr, R3 <: RingExpr]
//  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2], recur3: MakeExpr[X3,R3]):
//  MakeExpr[(X1,X2,X3),Tuple3RingExpr[R1,R2,R3]] =
//    new MakeExpr[(X1,X2,X3),Tuple3RingExpr[R1,R2,R3]] {
//      def apply(v1: (X1,X2,X3)) = Tuple3RingExpr(recur1(v1._1),recur2(v1._2),recur3(v1._3))
//    }

//  implicit def tuple2MakeKeyExpr[X1,X2,R1 <: KeyExpr, R2 <: KeyExpr]
//  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2KeyExpr[R1,R2]] =
//    new MakeExpr[(X1,X2),Tuple2KeyExpr[R1,R2]] {
//      def apply(v1: (X1,X2)) = Tuple2KeyExpr(recur1(v1._1),recur2(v1._2))
//    }
//
//  implicit def tuple3MakeKeyExpr[X1,X2,X3,R1 <: KeyExpr, R2 <: KeyExpr, R3 <: KeyExpr]
//  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2], recur3: MakeExpr[X3,R3]):
//  MakeExpr[(X1,X2,X3),Tuple3KeyExpr[R1,R2,R3]] =
//    new MakeExpr[(X1,X2,X3),Tuple3KeyExpr[R1,R2,R3]] {
//      def apply(v1: (X1,X2,X3)) = Tuple3KeyExpr(recur1(v1._1),recur2(v1._2),recur3(v1._3))
//    }

  implicit def productMakeRingExpr[P <: Product,PRepr <: HList, KRepr <: HList]
  (implicit gen: Generic.Aux[P,PRepr],
   make: HMakeExpr[PRepr,KRepr],
   trav: ToTraversable.Aux[KRepr,List,RingExpr]): MakeExpr[P,ProductRingExpr[KRepr]] =
    new MakeExpr[P,ProductRingExpr[KRepr]] {
      def apply(v1: P): ProductRingExpr[KRepr] = ProductRingExpr(make(gen.to(v1)))
    }

  implicit def productMakeKeyExpr[P <: Product,PRepr <: HList, KRepr <: HList]
    (implicit gen: Generic.Aux[P,PRepr],
              make: HMakeExpr[PRepr,KRepr],
              trav: ToTraversable.Aux[KRepr,List,KeyExpr]): MakeExpr[P,ProductKeyExpr[KRepr]] =
    new MakeExpr[P,ProductKeyExpr[KRepr]] {
      def apply(v1: P): ProductKeyExpr[KRepr] = ProductKeyExpr(make(gen.to(v1)))
    }


}

trait Priority2MakeExprImplicits extends Priority1MakeExprImplicits {
  //It's higher priority to make tuples of only variables into tuple variable expressions so that expressions
  //can be resolved. It also means they have access to the <-- method needed to make For comprehensions look nice.
  implicit def tuple2MakeVariableExpr[X1,X2,R1 <: VariableExpr[R1], R2 <: VariableExpr[R2]]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2]): MakeExpr[(X1,X2),Tuple2VariableExpr[R1,R2]] =
    new MakeExpr[(X1,X2),Tuple2VariableExpr[R1,R2]] {
      def apply(v1: (X1,X2)) = Tuple2VariableExpr(recur1(v1._1),recur2(v1._2))
    }

  implicit def tuple3MakeVariableExpr[X1,X2,X3,R1 <: VariableExpr[R1], R2 <: VariableExpr[R2], R3 <: VariableExpr[R3]]
  (implicit recur1: MakeExpr[X1,R1], recur2: MakeExpr[X2,R2], recur3: MakeExpr[X3,R3]):
  MakeExpr[(X1,X2,X3),Tuple3VariableExpr[R1,R2,R3]] =
    new MakeExpr[(X1,X2,X3),Tuple3VariableExpr[R1,R2,R3]] {
      def apply(v1: (X1,X2,X3)) = Tuple3VariableExpr(recur1(v1._1),recur2(v1._2),recur3(v1._3))
    }
}

trait MakeKeyRingPair[X,K <: KeyExpr,R <: RingExpr] extends (X => KeyRingPair[K,R])

object MakeKeyRingPair {

  implicit def IdMakeKeyRingPair[K <: KeyExpr, R <: RingExpr]: MakeKeyRingPair[KeyRingPair[K,R],K,R] =
    new MakeKeyRingPair[KeyRingPair[K,R],K,R] { def apply(v1: KeyRingPair[K,R]): KeyRingPair[K,R] = v1 }

  implicit def ImplicitOne[X, K <: KeyExpr](implicit make: MakeExpr[X,K]): MakeKeyRingPair[X,K,NumericExpr[Int]] =
    new MakeKeyRingPair[X,K,NumericExpr[Int]] {
      def apply(v1: X): KeyRingPair[K,NumericExpr[Int]] = KeyRingPair(make(v1),NumericExpr(1))
    }
}

trait Syntax {

  def toExpr[X, E <: Expr](x: X)(implicit make: MakeExpr[X,E]): E = make(x)

  implicit def toExprOps[X, E <: Expr](x: X)(implicit make: MakeExpr[X, E]): ExprOps[E] = ExprOps(make(x))
  implicit def toKeyExprOps[X, E <: KeyExpr](x: X)(implicit make: MakeExpr[X, E]): KeyExprOps[E] = KeyExprOps(make(x))
  implicit def toRingExprOps[X, E <: RingExpr](x: X)(implicit make: MakeExpr[X, E]): RingExprOps[E] = RingExprOps(make(x))
  implicit def toVariableExprOps[X, E <: VariableExpr[E]](x: X)(implicit make: MakeExpr[X, E]): VariableExprOps[E] = VariableExprOps(make(x))

  def Sum[R <: RingExpr](r: R): SumExpr[R] = SumExpr(r)

  def Sng[K <: KeyExpr, R <: RingExpr](k: K, r: R): SngExpr[K, R] = SngExpr(k, r)

  def Sng[T, K <: KeyExpr](t: T)(implicit make: MakeExpr[T, K]): SngExpr[K, NumericExpr[Int]] =
    SngExpr(make(t), NumericExpr(1))

  def Group[T, R <: RingExpr](t: T)(implicit make: MakeExpr[T,R]): GroupExpr[R] = GroupExpr(make(t))

  def toK[E <: RingExpr](e: E): BoxedRingExpr[E] = BoxedRingExpr(e)

  def toRing[E <: Expr](e: E): ToRingExpr[E] = ToRingExpr(e)


  object For {
    def apply[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](vrp: VariableRingPredicate[V,R,P]):
      ForComprehensionBuilder[V, R, P] = ForComprehensionBuilder(vrp)

    def apply[
      V1 <: VariableExpr[V1], V2 <: VariableExpr[V2], R1 <: RingExpr, R2 <: RingExpr, P1 <: RingExpr, P2 <: RingExpr
    ](vrp1: VariableRingPredicate[V1,R1,P1],
      vrp2: VariableRingPredicate[V2,R2,P2]): NestedForComprehensionBuilder[V1, V2, R1, R2, P1, P2] = {
      NestedForComprehensionBuilder(ForComprehensionBuilder(vrp1), ForComprehensionBuilder(vrp2))
    }
  }

  implicit class IfImplicit[V <: VariableExpr[V], R <: RingExpr, P <: RingExpr](pair: VariableRingPredicate[V,R,P]) {
    def If[P2 <: RingExpr](p: P2): VariableRingPredicate[V,R,P2] = VariableRingPredicate(pair.k,pair.r,p)
  }

}