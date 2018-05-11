package slender.execution

import slender._

import scala.tools.reflect.ToolBox
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

trait ExecutionContext[T] {
  def get(ref: String): T //generate the code to access the physical collection 'ref'
  def eval(tree: T): Any //evaluate code
}

trait ToolboxExecutionContext extends ExecutionContext[Tree] {
  val tb = currentMirror.mkToolBox()
  def me: String = this.getClass.getName.replace("$", "")
  def get(ref: String) = tb.parse(s"$me.$ref")
  def eval(tree: Tree): Any = tb.eval(tree)
}

trait CodeGen[T] {
  def apply(expr: RingExpr)(implicit ctx: ExecutionContext[T]): T
  def typeTree(exprType: ExprType): T
  def zero(ringType: RingType): T
  def add(ringType: RingType): T
  //def multiply(ringType: RingType): Tree
//  def dot(ringType: RingType): Tree
//  def negate(ringType: RingType): Tree
}

object LocalCodeGen extends CodeGen[Tree] {

  def apply(expr: RingExpr)(implicit ctx: ExecutionContext[Tree]): Tree = expr match {

    case IntExpr(i) => q"""$i"""

    case PhysicalCollection(kt, vt, ref) => q"""${ctx.get(ref)}"""

    case PhysicalBag(kt, ref) => q"""${ctx.get(ref)}""" //todo

    case Sum(c) => {
      val inner = apply(c) //must be a Map[(_,_)] since it has to be a finite mapping
      val zero = sumZero(c.exprType)
      val add = LocalCodeGen.add(c.exprType.asInstanceOf[MappingType].r)
      q"""$inner.values.foldRight($zero)($add)"""
    }

    case Add(c1, c2) => {
      val inner1 = apply(c1)
      val inner2 = apply(c2)
      val add = LocalCodeGen.add(c1.exprType)
      q"""$add($inner1, $inner2)"""
    }

    //    case Multiply(c1,c2) => {
    //      val inner1 = apply(c1)
    //      val inner2 = apply(c2)
    //      val multiply = LocalCodeGen.multiply(c1.exprType, c2.exprType)
    //    }

  }



  def typeTree(exprType: ExprType): Tree = exprType match {
    case IntType => tq"""Int"""
    case DomKeyType(t) => Ident(t.typeSymbol)
    case t: Tuple2ExprType => tq"""scala.Tuple2[${typeTree(t.t1)},${typeTree(t.t2)}]"""
    case t: Tuple3ExprType => tq"""scala.Tuple3[${typeTree(t.t1)},${typeTree(t.t2)},${typeTree(t.t2)}]"""
    case MappingType(k, v) => tq"""scala.collection.immutable.Map[${typeTree(k)},${typeTree(v)}]"""
  }

  def sumZero(ringType: RingType): Tree = ringType match {
    case MappingType(_, vT) => zero(vT)
  }

  def zero(ringType: RingType): Tree = ringType match {
    case IntType => q"""0"""
    case RingPairType(t1, t2) => q"""(${zero(t1)},${zero(t2)})"""
    case RingTuple3Type(t1, t2, t3) => q"""(${zero(t1)},${zero(t2)},${zero(t3)})"""
    case MappingType(k, v) => q"""List.empty[${typeTree(k)},${typeTree(v)}]"""
  }

  def add(ringType: RingType): Tree = ringType match {

    case IntType => q"""(x1:${typeTree(IntType)}, x2:${typeTree(IntType)}) => x1 + x2"""

    case t@RingPairType(t1, t2) =>
      q"""
       (x1:${typeTree(t)},x2:${typeTree(t)}) =>
       (${add(t1)}(x1._1,x2._1), ${add(t2)}(x1._2,x2._2))
    """

    case t@RingTuple3Type(t1, t2, t3) =>
      q"""
     (x1:${typeTree(t)},x2:${typeTree(t)}) =>
     (${add(t1)}(x1._1,x2._1), ${add(t2)}(x1._2,x2._2), ${add(t3)}(x1._3,x2._3))
   """

    case t@MappingType(kt, rt) =>
      q"""
    (x1:${typeTree(t)},x2:${typeTree(t)}) =>
    x1 ++ x2 map { case (k,v) => k -> ${add(rt)}(v,x1.getOrElse(k,${zero(rt)})) }
   """
  }

  //  def multiply(t1: RingType, t2: RingType): Tree = (t1,t2) match {
  //
  //    case
  //  }

}



trait Executor[T] {
  def ctx: ExecutionContext[T]
  def codeGen: CodeGen[T]
  def execute(expr: RingExpr) = ctx.eval(codeGen(expr)(ctx))
}

case class LocalExecutor(ctx: ExecutionContext[Tree], codeGen: CodeGen[Tree] = LocalCodeGen) extends Executor[Tree]