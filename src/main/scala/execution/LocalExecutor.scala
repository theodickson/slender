package slender.execution

import slender._

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe.{Expr => UniverseExpr,_}
import scala.tools.reflect.ToolBox


trait ExecutionContext[T] {
  def apply(ref: String): T
}


trait CodeGen[T] {
  def apply(expr: Expr[_])(implicit ctx: ExecutionContext[T]): T
  def typeTree(exprType: ExprType[_]): T
  def zero(t: RingType): T
  def add(t1: RingType, t2: RingType): T
  def multiply(t1: RingType, t2: RingType): T
  def dot(t1: RingType, t2: RingType): T
  def negate(t: RingType): T
}


trait Executor[T] {
  def apply(t: T): Any
}


trait Interpreter[T] {
  implicit def ctx: ExecutionContext[T]
  def codeGen: CodeGen[T]
  def executor: Executor[T]
  def apply(expr: RingExpr): Any = executor(codeGen(expr))
  def display(expr: RingExpr): String
}


trait ToolboxExecutionContext extends ExecutionContext[Tree] {
  def myTree: Tree = {
    val myPath = this.getClass.getName.replace("$", "").split('.')
    myPath.tail.foldLeft[Tree](Ident(TermName(myPath.head))) { (sel, name) =>
      Select(sel, TermName(name))
    }
  }
//  def apply(ref: String) = {
//    val path = ref.split('.')
//    assert(path.length > 0)
//    path.tail.foldLeft[Tree](Ident(TermName(path.head))) { (sel, name) =>
//      Select(sel, TermName(name))
//    }
//  }
  def apply(ref: String) = Select(myTree, TermName(ref))
}


object LocalCodeGen extends CodeGen[Tree] {

  def apply(expr: Expr[_])(implicit ctx: ExecutionContext[Tree]): Tree = expr match {

    case p : PrimitiveExpr[_, _] => p.literal

    case p : ProjectExpr[_] => Select(apply(p.c1), TermName(s"_${p.n}"))

    case p : ProductExpr[_] => tuple(p.children.map(apply))

    case PhysicalCollection(kt, vt, ref) => q"${ctx.apply(ref)}"

    case Sum(c) => unaryApply(sum(c.exprType))(c)

    case Add(c1, c2) => binaryApply(add(c1.exprType, c2.exprType))(c1, c2)

    case Multiply(c1:MappingExpr,inf:InfiniteMappingExpr) => {
      val lhs = apply(c1)
      val rhs = apply(inf)
      val dotFunc = dot(c1.exprType.asInstanceOf[MappingType].r, inf.valueType)
      q"$lhs.map { case (k,v1) => k -> $dotFunc(v1,$rhs(k)) }"
    }

    case Multiply(c1,c2) => binaryApply(multiply(c1.exprType,c2.exprType))(c1, c2)

    case Dot(c1,c2) => binaryApply(dot(c1.exprType,c2.exprType))(c1, c2)

    case Negate(c) => unaryApply(negate(c.exprType))(c)

    case Not(c) => unaryApply(not(c.exprType))(c)

    case Sng(k,v) => binaryApply(sng(k.exprType,v.exprType))(k,v)

    case InfiniteMappingExpr(key,value) => {
      val arg = valDef(key.exprType,key.asInstanceOf[VariableKeyExpr].name)
      val body = apply(value)
      Function(List(arg), body)
    }

    case TypedVariableKeyExpr(name,_) => Ident(TermName(name))

    case EqualsPredicate(c1,c2) => q"if (${apply(c1)} == ${apply(c2)}) 1 else 0"

    case BoxedRingExpr(c1) => apply(c1)

  }

  def unaryApply(func: Tree)(c1: Expr[_])(implicit ctx: ExecutionContext[Tree]): Tree = q"$func(${apply(c1)})"

  def binaryApply(func: Tree)(c1: Expr[_], c2: Expr[_])(implicit ctx: ExecutionContext[Tree]): Tree =
    q"$func(${apply(c1)},${apply(c2)})"

  def typeTree(exprType: ExprType[_]): Tree = exprType match {
    case t : PrimitiveType[_] => Ident(t.tpe.typeSymbol)
    case FiniteMappingType(k, v) => tq"scala.collection.immutable.Map[${typeTree(k)},${typeTree(v)}]"
    case InfiniteMappingType(k, v) => tq"${typeTree(k)} => ${typeTree(v)}"
    case t: ProductExprType[_] => {
      val ident = Select(Ident(TermName("scala")), TypeName(s"Tuple${t.ts.length}"))
      val args = t.ts.map(typeTree)
      AppliedTypeTree(ident, args.toList)
    }
    case BoxedRingType(r) => typeTree(r)
    case _ : UnresolvedExprType[_] => throw UnresolvedExprTypeException("Cannot make type tree for unresolved expr type.")
  }

  def sum(t: RingType): Tree = anonFunc(t) {
    t match {
      case FiniteMappingType(_, vT) => {
        val acc = zero(vT)
        val combine = add(vT, vT)
        q"x1.values.foldRight($acc)($combine)"
      }
      case _ => throw new IllegalStateException(s"Cannot sum ring of type $t")
    }
  }

  def negate(t: RingType): Tree = anonFunc(t) {
    t match {
      case IntType => q"-x1"
      case FiniteMappingType(k,v) => q"x1 mapValues ${negate(v)}"
      case p : ProductRingType => tuple(p.ts.map(negate))
      case _ : UnresolvedExprType[_] => throw UnresolvedExprTypeException("Cannot negate unresolved type expression.")
      case _ : InfiniteMappingType => throw new IllegalStateException("Cannot negate infinite mapping type.")
    }
  }

  def not(t: RingType): Tree = anonFunc(t) {
    t match {
      case IntType => q"if (x1 == 0) 1 else 0"
      case FiniteMappingType(k,v) => q"x1 mapValues ${negate(v)}"
      case p : ProductRingType => tuple(p.ts.map(negate))
      case _ : UnresolvedExprType[_] => throw UnresolvedExprTypeException("Cannot not unresolved type expression.")
      case _ : InfiniteMappingType => throw new IllegalStateException("Cannot not infinite mapping type.")
    }
  }

  def zero(t: RingType): Tree = t match {
    case IntType => q"0"
    case FiniteMappingType(k, v) => q"""Map.empty[${typeTree(k)},${typeTree(v)}]"""
    case p : ProductRingType => tuple(p.ts.map(zero))
    case _ : UnresolvedExprType[_] => throw new IllegalStateException("No zero for unresolved type.")
    case _ : InfiniteMappingType => throw new IllegalStateException("No zero infinite mapping type.")
  }

  def add(t1: RingType, t2: RingType): Tree = anonFunc(t1, t2) {
    (t1,t2) match {

      case (IntType,IntType) => q"x1 + x2"

      case (t@FiniteMappingType(_, rt),t2) if t == t2 =>
        q"x1 ++ x2.map { case (k,v) => k -> ${add(rt,rt)}(v,x1.getOrElse(k,${zero(rt)})) }"

      case (p:ProductRingType,p2:ProductRingType) if (p == p2) => {
        val elems = p.ts.zipWithIndex.map { case (t,i) =>
          val func = add(t,t)
          Apply(
            func,
            List(
              Select(Ident(TermName("x1")), TermName(s"_${i+1}")),
              Select(Ident(TermName("x2")), TermName(s"_${i+1}"))
            )
          )
        }
        tuple(elems)
      }

      case _ => throw new IllegalStateException(s"Cannot add rings of type $t1 and $t2")
    }
  }

  def multiply(t1: RingType, t2: RingType): Tree = anonFunc(t1,t2) {
    (t1,t2) match {

      case (IntType,IntType) => q"x1 * x2"

      case (r1:ProductRingType,r2:ProductRingType) if (r1 == r2) => ???

      case (FiniteMappingType(k1,v1),FiniteMappingType(k2,v2)) if (k1 == k2 && v1 == v2) =>
        q"""
           x1 map { case (k1,v1) => k1 -> ${multiply(v1,v2)}(v1,x2.getOrElse(k1,${zero(v1)})) }
         """

      case (FiniteMappingType(k1,v1),FiniteMappingType(k2,v2)) if (k1 == k2) => //TODO - do missing keys work the same as product wrt zero?
        q"""
           x1 map { case (k1,v1) => k1 -> ${dot(v1, v2)}(v1,x2.getOrElse(k1,${zero(v2)})) }
         """
      case _ => throw new IllegalStateException(s"Cannot multiply rings of type $t1 and $t2")
    }
  }

  def dot(t1: RingType, t2: RingType): Tree = anonFunc(t1, t2) {
    (t1,t2) match {

      case (IntType,IntType) => q"x1 * x2"

      case (r:ProductRingType,IntType) => {
        val elems = r.ts.zipWithIndex.map { case (t,i) =>
          val func = dot(t,IntType)
          Apply(
            func,
            List(
              Select(Ident(TermName("x1")), TermName(s"_${i+1}")),
              Ident(TermName("x2"))
            )
          )
        }
        tuple(elems)
      }

      case (IntType,r:ProductRingType) => {
        val elems = r.ts.zipWithIndex.map { case (t,i) =>
          val func = dot(IntType,t)
          Apply(
            func,
            List(
              Ident(TermName("x1")),
              Select(Ident(TermName("x2")), TermName(s"_${i+1}"))
            )
          )
        }
        tuple(elems)
      }

      case (FiniteMappingType(_,vT),IntType) => {
        val innerDot = dot(vT,IntType)
        q"x1.mapValues(v => $innerDot(v, x2))"
      }

      case (IntType,FiniteMappingType(_,vT)) => {
        val innerDot = dot(IntType,vT)
        q"x2.mapValues(v => $innerDot(x1, v))"
      }

      case (FiniteMappingType(_,vT),r:ProductRingType) => {
        val innerDot = dot(vT,r)
        q"x1.mapValues(v => $innerDot(v, x2))"
      }

      case (r:ProductRingType,FiniteMappingType(_,vT)) => {
        val innerDot = dot(r,vT)
        q"x2.mapValues(v => $innerDot(x1, v))"
      }

      case (FiniteMappingType(k1,v1),FiniteMappingType(k2,v2)) => {
        val innerDot = dot(v1,v2)
        q"""
          x1.flatMap { case (k1,v1) =>
           x2.map { case (k2,v2) => (k1,k2) -> $innerDot(v1,v2) }
         }
          """
      }
      case _ => throw new IllegalStateException(s"Cannot dot rings of type $t1 and $t2")
    }
  }

  def sng(k: KeyType, v: RingType): Tree = anonFunc(k, v) {
    (k,v) match {
      case (_,InfiniteMappingType(_,_)) => throw new IllegalStateException("Cannot have infinite mapping as value of sng")
      case _ => q"scala.collection.immutable.Map(x1 -> x2)"
    }
  }

  private def tuple(ts: Seq[Tree]): Tree = {
    val constructor = Ident(TermName(s"Tuple${ts.length}"))
    Apply(constructor, ts.toList)
  }

  private def valDef(t: ExprType[_], n: String): ValDef =
    ValDef(Modifiers(), TermName(n), typeTree(t), EmptyTree)

  private def anonFunc(ts: ExprType[_]*)(body: Tree): Function = {
    val valDefs = ts.zipWithIndex.map { case (t: ExprType[_], i: Int) => valDef(t, s"x${i+1}") }.toList
    Function(valDefs, body)
  }
}


object ToolBoxExecutor extends Executor[Tree] {
  val tb = currentMirror.mkToolBox()
  def apply(t: Tree) = tb.eval(t)
}


case class LocalInterpreter(ctx: ExecutionContext[Tree]) extends Interpreter[Tree] {
  val codeGen = LocalCodeGen
  val executor = ToolBoxExecutor
  def display(expr: RingExpr): String = showCode(codeGen(expr)(ctx))
}

case class UnresolvedExprTypeException(msg: String) extends Exception(msg)