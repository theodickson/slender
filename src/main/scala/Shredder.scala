package slender

import java.io.Serializable

trait Shredder[T <: Expr, S <: Expr] extends (T => S) with Serializable

trait NonShredder[T <: Expr] extends Shredder[T,T] {
  def apply(v1: T): T = v1
}