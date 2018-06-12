package slender

sealed trait _C extends UntypedVariable[_C] { override def name = "C" }
sealed trait _C1 extends UntypedVariable[_C1] { override def name = "C1" }
sealed trait _C2 extends UntypedVariable[_C2] { override def name = "C2" }
sealed trait _C3 extends UntypedVariable[_C3] { override def name = "C3" }

sealed trait _O extends UntypedVariable[_O] { override def name = "O" }
sealed trait _O1 extends UntypedVariable[_O1] { override def name = "O1" }
sealed trait _O2 extends UntypedVariable[_O2] { override def name = "O2" }
sealed trait _O3 extends UntypedVariable[_O3] { override def name = "O3" }

sealed trait _P extends UntypedVariable[_P] { override def name = "P" }
sealed trait _P1 extends UntypedVariable[_P1] { override def name = "P1" }
sealed trait _P2 extends UntypedVariable[_P2] { override def name = "P2" }
sealed trait _P3 extends UntypedVariable[_P3] { override def name = "P3" }

sealed trait _S extends UntypedVariable[_S] { override def name = "S" }
sealed trait _S1 extends UntypedVariable[_S1] { override def name = "S1" }
sealed trait _S2 extends UntypedVariable[_S2] { override def name = "S2" }
sealed trait _S3 extends UntypedVariable[_S3] { override def name = "S3" }

sealed trait _W extends UntypedVariable[_W] { override def name = "W" }
sealed trait _W1 extends UntypedVariable[_W1] { override def name = "W1" }
sealed trait _W2 extends UntypedVariable[_W2] { override def name = "W2" }
sealed trait _W3 extends UntypedVariable[_W3] { override def name = "W3" }

sealed trait _X extends UntypedVariable[_X] { override def name = "X" }
sealed trait _X1 extends UntypedVariable[_X1] { override def name = "X1" }
sealed trait _X2 extends UntypedVariable[_X2] { override def name = "X2" }
sealed trait _X3 extends UntypedVariable[_X3] { override def name = "X3" }

sealed trait _Y extends UntypedVariable[_Y] { override def name = "Y" }
sealed trait _Y1 extends UntypedVariable[_Y1] { override def name = "Y1" }
sealed trait _Y2 extends UntypedVariable[_Y2] { override def name = "Y2" }
sealed trait _Y3 extends UntypedVariable[_Y3] { override def name = "Y3" }

sealed trait _Z extends UntypedVariable[_Z] { override def name = "Z" }
sealed trait _Z1 extends UntypedVariable[_Z1] { override def name = "Z1" }
sealed trait _Z2 extends UntypedVariable[_Z2] { override def name = "Z2" }
sealed trait _Z3 extends UntypedVariable[_Z3] { override def name = "Z3" }

sealed trait _K extends UntypedVariable[_K] { override def name = "K" }
sealed trait _K1 extends UntypedVariable[_K1] { override def name = "K1" }
sealed trait _K2 extends UntypedVariable[_K2] { override def name = "K2" }
sealed trait _K3 extends UntypedVariable[_K3] { override def name = "K3" }


trait Variables {

  val C = new _C {}
  val C1 = new _C1 {}
  val C2 = new _C2 {}
  val C3 = new _C3 {}

  val O = new _O {}
  val O1 = new _O1 {}
  val O2 = new _O2 {}
  val O3 = new _O3 {}

  val P = new _P {}
  val P1 = new _P1 {}
  val P2 = new _P2 {}
  val P3 = new _P3 {}

  val S = new _S {}
  val S1 = new _S1 {}
  val S2 = new _S2 {}
  val S3 = new _S3 {}

  val X = new _X {}
  val X1 = new _X1 {}
  val X2 = new _X2 {}
  val X3 = new _X3 {}

  val Y = new _Y {}
  val Y1 = new _Y1 {}
  val Y2 = new _Y2 {}
  val Y3 = new _Y3 {}

  val Z = new _Z {}
  val Z1 = new _Z1 {}
  val Z2 = new _Z2 {}
  val Z3 = new _Z3 {}

  val W = new _W {}
  val W1 = new _W1 {}
  val W2 = new _W2 {}
  val W3 = new _W3 {}

  val K = new _K {}
  val K1 = new _K1 {}
  val K2 = new _K2 {}
  val K3 = new _K3 {}

}

