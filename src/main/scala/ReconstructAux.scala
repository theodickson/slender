package slender

import shapeless.{HList, HNil, ::}

trait ReconstructAux[In, Args <: HList, Reconstructed]

trait ReconstructionImplicits {
  implicit def reconstruct1[
  C[_ <: Expr], In <: Expr, Out <: Expr
  ]: ReconstructAux[C[In], ::[Out, HNil], C[Out]] = new ReconstructAux[C[In], ::[Out, HNil], C[Out]] {}

  implicit def reconstruct2[
  C[_ <: Expr, _ <: Expr], In1 <: Expr, Out1 <: Expr, In2 <: Expr, Out2 <: Expr
  ]: ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct3[
  C[_ <: Expr, _ <: Expr, _ <: Expr], In1 <: Expr, Out1 <: Expr, In2 <: Expr, Out2 <: Expr, In3 <: Expr, Out3 <: Expr
  ]: ReconstructAux[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] =
    new ReconstructAux[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] {}

  implicit def reconstruct1Key[
  C[_ <: KeyExpr], In <: KeyExpr, Out <: KeyExpr
  ]: ReconstructAux[C[In], ::[Out, HNil], C[Out]] = new ReconstructAux[C[In], ::[Out, HNil], C[Out]] {}

  implicit def reconstruct2Key[
  C[_ <: KeyExpr, _ <: KeyExpr], In1 <: KeyExpr, Out1 <: KeyExpr, In2 <: KeyExpr, Out2 <: KeyExpr
  ]: ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct3Key[
  C[_ <: KeyExpr, _ <: KeyExpr, _ <: KeyExpr], In1 <: KeyExpr, Out1 <: KeyExpr, In2 <: KeyExpr, Out2 <: KeyExpr, In3 <: KeyExpr, Out3 <: KeyExpr
  ]: ReconstructAux[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] =
    new ReconstructAux[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] {}

  implicit def reconstruct1Ring[
  C[_ <: RingExpr], In <: RingExpr, Out <: RingExpr
  ]: ReconstructAux[C[In], ::[Out, HNil], C[Out]] = new ReconstructAux[C[In], ::[Out, HNil], C[Out]] {}

  implicit def reconstruct2Ring[
  C[_ <: RingExpr, _ <: RingExpr], In1 <: RingExpr, Out1 <: RingExpr, In2 <: RingExpr, Out2 <: RingExpr
  ]: ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct3Ring[
  C[_ <: RingExpr, _ <: RingExpr, _ <: RingExpr], In1 <: RingExpr, Out1 <: RingExpr, In2 <: RingExpr, Out2 <: RingExpr, In3 <: RingExpr, Out3 <: RingExpr
  ]: ReconstructAux[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] =
    new ReconstructAux[C[In1, In2, In3], Out1 :: Out2 :: Out3 :: HNil, C[Out1, Out2, Out3]] {}

  implicit def reconstruct1Variable[
  C[X <: VariableExpr[X]], In <: VariableExpr[In], Out <: VariableExpr[Out]
  ]: ReconstructAux[C[In], ::[Out, HNil], C[Out]] = new ReconstructAux[C[In], ::[Out, HNil], C[Out]] {}

  implicit def reconstruct2Variable[
  C[X <: VariableExpr[X], Y <: VariableExpr[Y]], In1 <: VariableExpr[In1], Out1 <: VariableExpr[Out1], In2 <: VariableExpr[In2], Out2 <: VariableExpr[Out2]
  ]: ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct3Variable[
  C[X <: VariableExpr[X], Y <: VariableExpr[Y], Z <: VariableExpr[Z]],
  In1 <: VariableExpr[In1], Out1 <: VariableExpr[Out1], In2 <: VariableExpr[In2], Out2 <: VariableExpr[Out2],
  In3 <: VariableExpr[In3], Out3 <: VariableExpr[Out3]
  ]: ReconstructAux[C[In1, In2, In3], Out1::Out2::Out3::HNil, C[Out1, Out2, Out3]] =
    new ReconstructAux[C[In1, In2, In3], Out1::Out2::Out3::HNil, C[Out1, Out2, Out3]] {}

  implicit def reconstruct2KeyRing[
  C[_ <: KeyExpr, _ <: RingExpr], In1 <: KeyExpr, Out1 <: KeyExpr, In2 <: RingExpr, Out2 <: RingExpr
  ]: ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}

  implicit def reconstruct2VariableRing[
  C[X <: VariableExpr[X], _ <: RingExpr], In1 <: VariableExpr[In1], Out1 <: VariableExpr[Out1], In2 <: RingExpr, Out2 <: RingExpr
  ]: ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] =
    new ReconstructAux[C[In1, In2], Out1 :: Out2 :: HNil, C[Out1, Out2]] {}
}