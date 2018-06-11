package slender

import shapeless.{HList, HNil, ::}

trait Reconstruct[In, Args <: HList, Reconstructed]

object Reconstruct {
  implicit def reconstruct1[
  C[_ <: Expr], In <: Expr, Out <: Expr
  ]: Reconstruct[C[In], ::[Out, HNil], C[Out]] = new Reconstruct[C[In], ::[Out, HNil], C[Out]] {}

  implicit def reconstruct2[
  C[_ <: Expr, _ <: Expr], In1 <: Expr, Out1 <: Expr, In2 <: Expr, Out2 <: Expr
  ]: Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct3[
  C[_ <: Expr, _ <: Expr, _ <: Expr], In1 <: Expr, Out1 <: Expr, In2 <: Expr, Out2 <: Expr, In3 <: Expr, Out3 <: Expr
  ]: Reconstruct[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] =
    new Reconstruct[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] {}

  implicit def reconstruct1Key[
  C[_ <: KeyExpr], In <: KeyExpr, Out <: KeyExpr
  ]: Reconstruct[C[In], ::[Out, HNil], C[Out]] = new Reconstruct[C[In], ::[Out, HNil], C[Out]] {}

  implicit def reconstruct2Key[
  C[_ <: KeyExpr, _ <: KeyExpr], In1 <: KeyExpr, Out1 <: KeyExpr, In2 <: KeyExpr, Out2 <: KeyExpr
  ]: Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct3Key[
  C[_ <: KeyExpr, _ <: KeyExpr, _ <: KeyExpr], In1 <: KeyExpr, Out1 <: KeyExpr, In2 <: KeyExpr, Out2 <: KeyExpr, In3 <: KeyExpr, Out3 <: KeyExpr
  ]: Reconstruct[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] =
    new Reconstruct[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] {}

  implicit def reconstruct1Ring[
  C[_ <: RingExpr], In <: RingExpr, Out <: RingExpr
  ]: Reconstruct[C[In], ::[Out, HNil], C[Out]] = new Reconstruct[C[In], ::[Out, HNil], C[Out]] {}

  implicit def reconstruct2Ring[
  C[_ <: RingExpr, _ <: RingExpr], In1 <: RingExpr, Out1 <: RingExpr, In2 <: RingExpr, Out2 <: RingExpr
  ]: Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct3Ring[
  C[_ <: RingExpr, _ <: RingExpr, _ <: RingExpr], In1 <: RingExpr, Out1 <: RingExpr, In2 <: RingExpr, Out2 <: RingExpr, In3 <: RingExpr, Out3 <: RingExpr
  ]: Reconstruct[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] =
    new Reconstruct[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] {}

  implicit def reconstruct1Variable[
  C[X <: VariableExpr[X]], In <: VariableExpr[In], Out <: VariableExpr[Out]
  ]: Reconstruct[C[In], ::[Out, HNil], C[Out]] = new Reconstruct[C[In], ::[Out, HNil], C[Out]] {}

  implicit def reconstruct2Variable[
  C[X <: VariableExpr[X], Y <: VariableExpr[Y]], In1 <: VariableExpr[In1], Out1 <: VariableExpr[Out1], In2 <: VariableExpr[In2], Out2 <: VariableExpr[Out2]
  ]: Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct3Variable[
  C[X <: VariableExpr[X], Y <: VariableExpr[Y], Z <: VariableExpr[Z]],
  In1 <: VariableExpr[In1], Out1 <: VariableExpr[Out1], In2 <: VariableExpr[In2], Out2 <: VariableExpr[Out2],
  In3 <: VariableExpr[In3], Out3 <: VariableExpr[Out3]
  ]: Reconstruct[C[In1, In2, In3], Out1::Out2::Out3::HNil, C[Out1, Out2, Out3]] =
    new Reconstruct[C[In1, In2, In3], Out1::Out2::Out3::HNil, C[Out1, Out2, Out3]] {}

  implicit def reconstruct2KeyRing[
  C[_ <: KeyExpr, _ <: RingExpr], In1 <: KeyExpr, Out1 <: KeyExpr, In2 <: RingExpr, Out2 <: RingExpr
  ]: Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct2VariableRing[
  C[X <: VariableExpr[X], _ <: RingExpr], In1 <: VariableExpr[In1], Out1 <: VariableExpr[Out1], In2 <: RingExpr, Out2 <: RingExpr
  ]: Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new Reconstruct[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}
}