//package slender
//
//trait MyBinaryExpr[L <: Expr, R <: Expr] {
//  def c1: L
//  def c2: R
//}
//
//trait MemberBinaryExpr {
//  type C1
//  type C2
//  def c1: C1
//  def c2: C2
//}
//
//trait Rebuilder[-In <: Expr, +Out <: Expr] extends (In => Out)
//
//trait BinaryConstructor[B[_,_]] {
//  def apply[C1 <: Expr, C2 <: Expr](c1: C1, c2: C2): B[C1,C2]
//}
//
//trait BinaryConstructor[B <: MemberBinaryExpr] {
//  def apply[C1 <: Expr, C2 <: Expr](c1: C1, c2: C2): B { type C1 = C1; type C2 = C2 } = B
//}
//
//object RebuilderImplicits {
//  def BinaryRebuilder[C[_,_] <: MyBinaryExpr[L,R],L <: Expr, R <: Expr, L1 <: Expr, R1 <: Expr]
//  (implicit const: BinaryConstructor[C], rebuildL: Rebuilder[L,L1], rebuildR: Rebuilder[R,R1]): Rebuilder[C[L,R],C[L1,R1]] =
//    new Rebuilder[C[L,R],C[L1,R1]] {
//      def apply(v1: C[L,R]): C[L1,R1] = const(v1.c1,v1.c2)
//  }
//
//  def BinaryRebuilder[B <: MemberBinaryExpr]
//  (implicit const: BinaryConstructor[C], rebuildL: Rebuilder[L,L1], rebuildR: Rebuilder[R,R1]): Rebuilder[C[L,R],C[L1,R1]] =
//    new Rebuilder[C[L,R],C[L1,R1]] {
//      def apply(v1: C[L,R]): C[L1,R1] = const(v1.c1,v1.c2)
//    }
//}
