//package slender.execution
//
//import slender._
//
//import scala.reflect.runtime.currentMirror
//import scala.reflect.runtime.universe.{Expr => UniverseExpr, _}
//import scala.tools.reflect.ToolBox
//
//trait ExecutionContext {
//  def setup: Seq[Tree] = Seq.empty[Tree]
//  def tearDown: Seq[Tree] = Seq.empty[Tree]
//  def apply(ref: String): Tree
//}
//
//
//trait CodeGen {
//  def apply(expr: Expr[_])(implicit ctx: ExecutionContext): Tree = {
//    validate(expr)
//    q"""
//         ..${ctx.setup}
//         ..${predef(expr)}
//         val result = ${generate(expr)}
//         ..${ctx.tearDown}
//         result
//     """
//  }
//  def generate(expr: Expr[_])(implicit ctx: ExecutionContext): Tree
//  def predef(expr: Expr[_])(implicit ctx: ExecutionContext): Seq[Tree]
//  def validate(expr: Expr[_]): Unit = expr.variables.foreach { v =>
//    if (v.name.matches("_.*"))
//      throw new IllegalStateException(s"Expr contains variable named ${v.name} which is reserved for use by codegen.")
//  }
//  def typeTree(exprType: ExprType[_]): Tree
//  def zero(t: RingType): Tree
//  def add(t1: RingType, t2: RingType): Tree
//  def multiply(t1: RingType, t2: RingType): Tree
//  def dot(t1: RingType, t2: RingType): Tree
//  def negate(t: RingType): Tree
//}
//
//
//trait Executor {
//  def apply(t: Tree): Any
//}
//
//
//trait Interpreter {
//  implicit def ctx: ExecutionContext
//  def codeGen: CodeGen
//  def executor: Executor
//  def apply(expr: RingExpr): Any = executor(codeGen(expr))
//  def showProgram(expr: RingExpr): String
//}
//
//
//trait ToolboxExecutionContext extends ExecutionContext {
//  def myTree: Tree = {
//    val myPath = this.getClass.getName.replace("$", "").split('.')
//    myPath.tail.foldLeft[Tree](Ident(TermName(myPath.head))) { (sel, name) =>
//      Select(sel, TermName(name))
//    }
//  }
//  def apply(ref: String): Tree = Select(myTree, TermName(ref))
//}
//
//trait LocalSparkExecutionContext extends ToolboxExecutionContext {
//  override def setup = List(
//    //q"""val spark = org.apache.spark.sql.SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()""",
//    q"import slender.TestSparkExecutionContext.spark.implicits._"
//  )
//  val spark = org.apache.spark.sql.SparkSession.builder
//    .appName("test")
//    .config("spark.master", "local")
//    .getOrCreate()
//
////  override def apply(ref: String): Tree = {
////    val localCollection = super.apply(ref)//Select(myTree, TermName(ref))
////    Apply(Select(Ident(TermName("spark")), TermName("createDataset")), List(localCollection))
////  }
//  //override def tearDown = List(q"spark.stop")
//}
//
//class LocalCodeGen extends CodeGen {
//
//  val usedVars = scala.collection.mutable.Set[String]()
//
//  def freshVar: String = {
//    val attempt = s"_${scala.util.Random.alphanumeric.filter(_.isLetter).filter(_.isLower).take(3).mkString("")}"
//      if (usedVars.contains(attempt)) freshVar
//      else { usedVars += attempt; attempt }
//  }
//
//  def generate(expr: Expr[_])(implicit ctx: ExecutionContext): Tree = expr match {
//
//    case p : PrimitiveExpr[_,_] => p.literal
//
//    case p : ProjectExpr[_] => Select(generate(p.c1), TermName(s"_${p.n}"))
//
//    case p : ProductExpr[_] => tuple(p.children.map(generate))
//
//    case p : PhysicalCollection => ctx(p.ref)
//
//    case Sum(c) => unaryApply(sum(c.exprType))(c)
//
//    case Add(c1, c2) => binaryApply(add(c1.exprType, c2.exprType))(c1, c2)
//
//    case Multiply(c1:MappingExpr,inf:InfiniteMappingExpr) => {
//      val lhs = generate(c1)
//      val rhs = generate(inf)
//      val dotFunc = dot(c1.exprType.asInstanceOf[MappingType].r, inf.valueType)
//      q"$lhs.map { case (_k,_v1) => _k -> $dotFunc(_v1,$rhs(_k)) }"
//    }
//
//    case Multiply(c1,c2) => binaryApply(multiply(c1.exprType,c2.exprType))(c1, c2)
//
//    case Dot(c1,c2) => binaryApply(dot(c1.exprType,c2.exprType))(c1, c2)
//
//    case Negate(c) => unaryApply(negate(c.exprType))(c)
//
//    case Not(c) => unaryApply(not(c.exprType))(c)
//
//    case Sng(k,v) => binaryApply(sng(k.exprType,v.exprType))(k,v)
//
//    case InfiniteMappingExpr(key,value) => {
//      val varName = freshVar
//      val arg = valDef(key.exprType,varName)//key.asInstanceOf[Variable].name)
//      //extractor so can deal with nested free variables.
//      val extract = extractor(key.asInstanceOf[VariableKeyExpr], varName)
//      val body = q"..$extract; ${generate(value)}"
//      //println(body)
//      Function(List(arg), body)
//    }
//
//    case TypedVariable(name,_) => Ident(TermName(name))
//
//    case UntypedVariable(_) => ???
//
//    case EqualsPredicate(c1,c2) => q"if (${generate(c1)} == ${generate(c2)}) 1 else 0"
//
//    case p@IntPredicate(c1,c2,_,_) => q"if (${p.literal}(${generate(c1)},${generate(c2)})) 1 else 0"
//
//    case BoxedRingExpr(c1) => generate(c1)
//
//    case FromBoxedRing(c1) => generate(c1)
//
//    case l@LabelExpr(c1) => Apply(Ident(TermName(s"Label${l.id}")), c1.freeVariables.map(generate).toList)
//
//    case FromLabel(l) => q"${generate(l)}.get"
//
//  }
//
//  def predef(expr: Expr[_])(implicit ctx: ExecutionContext): Seq[Tree] =
//    if (expr.isShredded) predefShredded(expr)
//    else Seq.empty[Tree]
//
//  def predefShredded(expr: Expr[_])(implicit ctx: ExecutionContext) = {
//
//    val labelTrait =
//      q"""
//         trait Label[T] {
//          def get: T
//         }
//       """
//
//    val labelClasses = expr.labels.map { l =>
//      val name = TypeName(s"Label${l.id}")
//      val args = l.freeVariables.map {
//        case TypedVariable(n,t) => Typed(Ident(TermName(n)),typeTree(t))
//      } toList
//      val argsTree = args.size match {
//        case 0 => ???
//        case 1 => args.head
//        case _ => tuple(args)
//      }
//      val wrappedType = typeTree(l.c1.exprType)
//
//      q"""
//        case class $name($argsTree) extends Label[$wrappedType] {
//          def get = ${generate(l.c1)}
//        }
//       """
//    }
//
//
//    labelTrait +: (labelClasses)
//  }
//
//  def unaryApply(func: Tree)(c1: Expr[_])(implicit ctx: ExecutionContext): Tree = q"$func(${generate(c1)})"
//
//  def binaryApply(func: Tree)(c1: Expr[_], c2: Expr[_])(implicit ctx: ExecutionContext): Tree =
//    q"$func(${generate(c1)},${generate(c2)})"
//
//  def typeTree(exprType: ExprType[_]): Tree = exprType match {
//    case t : PrimitiveType[_] => Ident(t.tpe.typeSymbol)
//    case FiniteMappingType(k,v,false) => tq"scala.collection.immutable.Map[${typeTree(k)},${typeTree(v)}]"
//    case FiniteMappingType(k,v,true) => tq"org.apache.spark.sql.Dataset[${typeTree(k)},${typeTree(v)}]"
//    case InfiniteMappingType(k, v) => tq"${typeTree(k)} => ${typeTree(v)}"
//    case t: ProductExprType[_] => {
//      val ident = Select(Ident(TermName("scala")), TypeName(s"Tuple${t.ts.length}"))
//      val args = t.ts.map(typeTree)
//      AppliedTypeTree(ident, args.toList)
//    }
//    case BoxedRingType(r) => typeTree(r)
//    case LabelType(r) => AppliedTypeTree(Ident(TypeName("Label")), List(typeTree(r)))
//    case _ : UnresolvedExprType[_] => throw UnresolvedExprTypeException("Cannot make type tree for unresolved expr type.")
//  }
//
//  def sum(t: RingType): Tree = anonFunc(t) {
//    t match {
//      case FiniteMappingType(_,vT,false) => {
//        val acc = zero(vT)
//        val combine = add(vT, vT)
//        q"_x1.values.foldRight($acc)($combine)"
//      }
//      case _ => throw new IllegalStateException(s"Cannot sum ring of type $t")
//    }
//  }
//
//  def negate(t: RingType): Tree = anonFunc(t) {
//    t match {
//      case IntType => q"-_x1"
//      case FiniteMappingType(k,v,false) => q"_x1 mapValues ${negate(v)}"
//      case p : ProductRingType => tuple(p.ts.map(negate))
//      case _ : UnresolvedExprType[_] => throw UnresolvedExprTypeException("Cannot negate unresolved type expression.")
//      case _ : InfiniteMappingType => throw new IllegalStateException("Cannot negate infinite mapping type.")
//    }
//  }
//
//  def not(t: RingType): Tree = anonFunc(t) {
//    t match {
//      case IntType => q"if (_x1 == 0) 1 else 0"
//      case FiniteMappingType(k,v,false) => q"_x1 mapValues ${negate(v)}"
//      case p : ProductRingType => tuple(p.ts.map(negate))
//      case _ : UnresolvedExprType[_] => throw UnresolvedExprTypeException("Cannot not unresolved type expression.")
//      case _ : InfiniteMappingType => throw new IllegalStateException("Cannot not infinite mapping type.")
//    }
//  }
//
//  def zero(t: RingType): Tree = t match {
//    case IntType => q"0"
//    case FiniteMappingType(k,v,false) => q"""Map.empty[${typeTree(k)},${typeTree(v)}]"""
//    case p : ProductRingType => tuple(p.ts.map(zero))
//    case _ : UnresolvedExprType[_] => throw new IllegalStateException("No zero for unresolved type.")
//    case _ : InfiniteMappingType => throw new IllegalStateException("No zero infinite mapping type.")
//  }
//
//  def add(t1: RingType, t2: RingType): Tree = anonFunc(t1, t2) {
//    (t1,t2) match {
//
//      case (IntType,IntType) => q"_x1 + _x2"
//
//      case (t@FiniteMappingType(_,rt,false),t2) if t == t2 =>
//        q"_x1 ++ _x2.map { case (_k,_v) => _k -> ${add(rt,rt)}(_v,_x1.getOrElse(_k,${zero(rt)})) }"
//
//      case (p:ProductRingType,p2:ProductRingType) if (p == p2) => {
//        val elems = p.ts.zipWithIndex.map { case (t,i) =>
//          val func = add(t,t)
//          Apply(
//            func,
//            List(
//              Select(Ident(TermName("_x1")), TermName(s"_${i+1}")),
//              Select(Ident(TermName("_x2")), TermName(s"_${i+1}"))
//            )
//          )
//        }
//        tuple(elems)
//      }
//
//      case _ => throw new IllegalStateException(s"Cannot add rings of type $t1 and $t2")
//    }
//  }
//
//  def multiply(t1: RingType, t2: RingType): Tree = anonFunc(t1,t2) {
//    (t1,t2) match {
//
//      case (IntType,IntType) => q"_x1 * _x2"
//
//      case (r1:ProductRingType,r2:ProductRingType) if (r1 == r2) => ???
//
//      case (FiniteMappingType(k1,v1,false),FiniteMappingType(k2,v2,false)) if (k1 == k2 && v1 == v2) =>
//        q"""
//           _x1 map { case (_k1,_v1) => _k1 -> ${multiply(v1,v2)}(_v1,_x2.getOrElse(_k1,${zero(v1)})) }
//         """
//
//      case (FiniteMappingType(k1,v1,false),FiniteMappingType(k2,v2,false)) if (k1 == k2) => //TODO - do missing keys work the same as product wrt zero?
//        q"""
//           _x1 map { case (_k1,_v1) => _k1 -> ${dot(v1, v2)}(_v1,_x2.getOrElse(_k1,${zero(v2)})) }
//         """
//      case _ => throw new IllegalStateException(s"Cannot multiply rings of type $t1 and $t2")
//    }
//  }
//
//  def dot(t1: RingType, t2: RingType): Tree = anonFunc(t1, t2) {
//    (t1,t2) match {
//
//      case (IntType,IntType) => q"_x1 * _x2"
//
//      case (r:ProductRingType,IntType) => {
//        val elems = r.ts.zipWithIndex.map { case (t,i) =>
//          val func = dot(t,IntType)
//          Apply(
//            func,
//            List(
//              Select(Ident(TermName("_x1")), TermName(s"_${i+1}")),
//              Ident(TermName("_x2"))
//            )
//          )
//        }
//        tuple(elems)
//      }
//
//      case (IntType,r:ProductRingType) => {
//        val elems = r.ts.zipWithIndex.map { case (t,i) =>
//          val func = dot(IntType,t)
//          Apply(
//            func,
//            List(
//              Ident(TermName("_x1")),
//              Select(Ident(TermName("_x2")), TermName(s"_${i+1}"))
//            )
//          )
//        }
//        tuple(elems)
//      }
//
//      case (FiniteMappingType(_,vT,false),IntType) => {
//        val innerDot = dot(vT,IntType)
//        q"_x1.mapValues(_v => $innerDot(_v, _x2))"
//      }
//
//      case (IntType,FiniteMappingType(_,vT,false)) => {
//        val innerDot = dot(IntType,vT)
//        q"_x2.mapValues(_v => $innerDot(_x1, _v))"
//      }
//
//      case (FiniteMappingType(_,vT,false),r:ProductRingType) => {
//        val innerDot = dot(vT,r)
//        q"_x1.mapValues(_v => $innerDot(_v, _x2))"
//      }
//
//      case (r:ProductRingType,FiniteMappingType(_,vT,false)) => {
//        val innerDot = dot(r,vT)
//        q"_x2.mapValues(_v => $innerDot(_x1, _v))"
//      }
//
//      case (FiniteMappingType(k1,v1,false),FiniteMappingType(k2,v2,false)) => {
//        val innerDot = dot(v1,v2)
//        q"""
//          _x1.flatMap { case (_k1,_v1) =>
//           _x2.map { case (_k2,_v2) => (_k1,_k2) -> $innerDot(_v1,_v2) }
//         }
//          """
//      }
//      case _ => throw new IllegalStateException(s"Cannot dot rings of type $t1 and $t2")
//    }
//  }
//
//  def sng(k: KeyType, v: RingType): Tree = anonFunc(k, v) {
//    (k,v) match {
//      case (_,InfiniteMappingType(_,_)) => throw new IllegalStateException("Cannot have infinite mapping as value of sng")
//      case _ => q"scala.collection.immutable.Map(_x1 -> _x2)"
//    }
//  }
//
//  private def tuple(ts: Seq[Tree]): Tree = {
//    val constructor = Ident(TermName(s"Tuple${ts.length}"))
//    Apply(constructor, ts.toList)
//  }
//
//  private def valDef(t: ExprType[_], n: String): ValDef =
//    ValDef(Modifiers(), TermName(n), typeTree(t), EmptyTree)
//
//  private def anonFunc(ts: ExprType[_]*)(body: Tree): Function = {
//    val valDefs = ts.zipWithIndex.map { case (t: ExprType[_], i: Int) => valDef(t, s"_x${i+1}") }.toList
//    Function(valDefs, body)
//  }
//
//  private def extractor(v: VariableKeyExpr, varName: String): Tree = {
//    def go(v: VariableKeyExpr): String = v match {
//      case ProductVariableKeyExpr(cs) => s"(${cs.map(v => go(v.asInstanceOf[VariableKeyExpr])).mkString(",")})"
//      case va : Variable => va.name
//    }
//    //todo - remove dependency on toolbox
//    val ex = currentMirror.mkToolBox().parse(s"val ${go(v)} = $varName")
//    ex
//  }
//}
//
//
//object ToolBoxExecutor extends Executor {
//  val tb = currentMirror.mkToolBox()
//  def apply(t: Tree) = tb.eval(t)
//}
//
//
//case class LocalInterpreter(ctx: ExecutionContext) extends Interpreter {
//  val codeGen = new LocalCodeGen
//  val executor = ToolBoxExecutor
//  def showProgram(expr: RingExpr): String = showCode(codeGen(expr)(ctx))
//}
//
//case class UnresolvedExprTypeException(msg: String) extends Exception(msg)