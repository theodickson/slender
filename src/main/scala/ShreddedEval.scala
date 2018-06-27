package slender

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.NestedConstantListContext
import shapeless.{::, HList}

import scala.reflect.ClassTag

case class Label[T](value: T) extends Serializable

//trait ShreddedResult[Flat,Ctx] {
//  type Nested
//  def nested: Nested
//}

trait Nest[-Flat,-Ctx,+Nested] extends ((Flat,Ctx) => Nested) with Serializable

object Nest {

  def instance[Flat,Ctx,Nested](f: (Flat,Ctx) => Nested): Nest[Flat,Ctx,Nested] = new Nest[Flat,Ctx,Nested] {
    def apply(v1: Flat, v2: Ctx): Nested = f(v1,v2)
  }

  implicit def LiteralNest[T,Ctx]: Nest[LiteralExpr[T],Ctx,T] = instance {
    
  }
}

trait GroupLabeller[-In,+Out] extends (In => Out) with Serializable

object GroupLabeller {

  implicit def rdd[K1:ClassTag,K2 <: HList,R]: GroupLabeller[RDD[(K1::K2,R)],RDD[(K1,Label[K1])]] =
    new GroupLabeller[RDD[(K1::K2,R)],RDD[(K1,Label[K1])]] {
      def apply(v1: RDD[(K1::K2,R)]): RDD[(K1,Label[K1])] = v1.map(_._1.head).distinct.map(k1 => (k1,Label(k1)))
    }
}

trait ShreddingContext[C]

trait ShreddedEval[-Expr,+Out,+Ctx] extends ((Expr,BoundVars) => (Out,Ctx)) with Serializable

object ShreddedEval {

  def instance[E,T,C](f: (E,BoundVars) => (T,C)): ShreddedEval[E,T,C] = new ShreddedEval[E,T,C] {
    def apply(v1: E, v2: BoundVars): (T,C) = f(v1,v2)
  }

  implicit def literalShreddedEval[T]: ShreddedEval[LiteralExpr[T],T,Unit] = instance {
    case (LiteralExpr(t),_) => (t,())
  }

  implicit def groupShreddedEval[E,O,C,Labelled,OG]
  (implicit eval: ShreddedEval[E,O,C], label: GroupLabeller[O,Labelled], group: Group[O,OG]): ShreddedEval[GroupExpr[E],Labelled,(OG,C)] = instance {
    case (GroupExpr(e),bvs) => {
      val (eF,eC) = eval(e,bvs)
      //eF :: (K1,K2) --> R
      val labelled = label(eF) //(K1,Label[K1])
      val grouped = group(eF) //(Label[K1],(K2 --> R))
      (labelled,(grouped,eC))
    }
  }

  implicit def sumShreddedEval[E,O,C,O1](eval: ShreddedEval[E,O,C], sum: Sum[O,O1]): ShreddedEval[SumExpr[E],O1,C] =
    instance { case (SumExpr(e),bvs) => {
        val (eF,eC) = eval(e,bvs)
        (sum(eF),eC)
      }
    }
}

object ShreddedResult {
  implicit def unitContext
}