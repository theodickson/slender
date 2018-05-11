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
//  def multiply(ringType: RingType): T
//  def dot(ringType: RingType): T
//  def negate(ringType: RingType): T
}

object LocalCodeGen extends CodeGen[Tree] {

//  def printCode(expr: RingExpr)(implicit ctx: ExecutionContext[Tree]): Unit = {
//    val tree = apply(expr)
//    ctx.tb
//  }
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

    case Multiply(c1,c2) => {
      val inner1 = apply(c1)
      val inner2 = apply(c2)
      val multiply = LocalCodeGen.multiply(c1.exprType, c2.exprType)
      q"""$multiply($inner1, $inner2) """
    }

    case Dot(c1,c2) => {
      val inner1 = apply(c1)
      val inner2 = apply(c2)
      val dot = LocalCodeGen.dot(c1.exprType, c2.exprType)
      q"""$dot($inner1, $inner2)"""
    }

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

  def add(ringType: RingType): Tree = anonFunc(ringType, ringType) {
    ringType match {

      case IntType => q"""x1 + x2"""

      case t@RingPairType(t1, t2) => q"""(${add(t1)}(x1._1,x2._1), ${add(t2)}(x1._2,x2._2))"""

      case t@RingTuple3Type(t1, t2, t3) =>
        q"""(${add(t1)}(x1._1,x2._1), ${add(t2)}(x1._2,x2._2), ${add(t3)}(x1._3,x2._3))"""

      case t@MappingType(kt, rt) =>
        q"""x1 ++ x2 map { case (k,v) => k -> ${add(rt)}(v,x1.getOrElse(k,${zero(rt)})) }"""
    }
  }

  def multiply(t1: RingType, t2: RingType): Tree = anonFunc(t1,t2) {
    (t1,t2) match {

      case (IntType,IntType) => q"""x1 * x2"""

      case (MappingType(k1,v1),MappingType(k2,v2)) if (k1 == k2 && v1 == v2) =>
        q"""
           x1 map { case (k1,v1) => k1 -> ${multiply(v1,v2)}(v1,x2.getOrElse(k1,${zero(v1)})) }
         """

      case (MappingType(k1,v1),MappingType(k2,v2)) if (k1 == k2) => //TODO - do missing keys work the same as product wrt zero?
        q"""
           x1 map { case (k1,v1) => k1 -> ${dot(v1, v2)}(v1,x2.getOrElse(k1,${zero(v1)})) }
         """
    }
  }

  def dot(t1: RingType, t2: RingType): Tree = anonFunc(t1, t2) {
    (t1,t2) match {

      case (IntType,IntType) => q"""x1 * x2"""

      case (MappingType(_,valueType),IntType) => {
        val innerDot = dot(valueType,IntType)
        q"""x1.mapValues(v => $innerDot(v, x2))"""
      }

      case (IntType,MappingType(_,valueType)) => {
        val innerDot = dot(IntType,valueType)
        q"""x2.mapValues(v => $innerDot(v, x1))"""
      }

      case (MappingType(k1,v1),MappingType(k2,v2)) => {
        val innerDot = dot(v1,v2)
        q"""
          x1.flatMap { case (k1,v1) =>
           x2.map { case (k2,v2) => (k1,k2) -> $innerDot(v1,v2) }
         }
          """
      }
    }
  }

  private def valDef(t: ExprType, n: String): ValDef =
    ValDef(Modifiers(), TermName(n), typeTree(t), EmptyTree)

  private def anonFunc(ts: ExprType*)(body: Tree): Function = {
    val valDefs = ts.zipWithIndex.map { case (t: ExprType, i: Int) => valDef(t, s"x${i+1}") }.toList
    Function(valDefs, body)
  }
}



trait Executor[T] {
  def ctx: ExecutionContext[T]
  def codeGen: CodeGen[T]
  def execute(expr: RingExpr) = ctx.eval(codeGen(expr)(ctx))
//  def printCode(expr: RingExpr) = ctx.
}

case class LocalExecutor(ctx: ExecutionContext[Tree], codeGen: CodeGen[Tree] = LocalCodeGen) extends Executor[Tree]