// Copyright 2017 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commbank.grimlock.framework

import commbank.grimlock.framework.encoding.{
  BooleanValue,
  DoubleValue,
  IntValue,
  LongValue,
  StringValue,
  TypeValue,
  Value
}
import commbank.grimlock.framework.metadata.Type

import shapeless.{ ::, HNil }

package environment {
  package object implicits {
    /** Converts a `Boolean` to a `Value`. */
    implicit def booleanToValue(t: Boolean): Value[Boolean] = BooleanValue(t)

    /** Converts a `Double` to a `Value`. */
    implicit def doubleToValue(t: Double): Value[Double] = DoubleValue(t)

    /** Converts a `Int` to a `Value`. */
    implicit def intToValue(t: Int): Value[Int] = IntValue(t)

    /** Converts a `Long` to a `Value`. */
    implicit def longToValue(t: Long): Value[Long] = LongValue(t)

    /** Converts a `String` to a `Value`. */
    implicit def stringToValue(t: String): Value[String] = StringValue(t)

    /** Converts a `Type` to a `Value`. */
    implicit def typeToValue(t: Type): Value[Type] = TypeValue(t)
  }
}

package object position {
  /** Short hand for 1 coordinate. */
  type Coordinates1[T1] = Value[T1] :: HNil

  /** Short hand for 2 coordinates. */
  type Coordinates2[T1, T2] = Value[T1] :: Value[T2] :: HNil

  /** Short hand for 3 coordinates. */
  type Coordinates3[T1, T2, T3] = Value[T1] :: Value[T2] :: Value[T3] :: HNil

  /** Short hand for 4 coordinates. */
  type Coordinates4[T1, T2, T3, T4] = Value[T1] :: Value[T2] :: Value[T3] :: Value[T4] :: HNil

  /** Short hand for 5 coordinates. */
  type Coordinates5[T1, T2, T3, T4, T5] = Value[T1] :: Value[T2] :: Value[T3] :: Value[T4] :: Value[T5] :: HNil

  /** Short hand for 6 coordinates. */
  type Coordinates6[T1, T2, T3, T4, T5, T6] = Value[T1] ::
    Value[T2] ::
    Value[T3] ::
    Value[T4] ::
    Value[T5] ::
    Value[T6] ::
    HNil

  /** Short hand for 7 coordinates. */
  type Coordinates7[T1, T2, T3, T4, T5, T6, T7] = Value[T1] ::
    Value[T2] ::
    Value[T3] ::
    Value[T4] ::
    Value[T5] ::
    Value[T6] ::
    Value[T7] ::
    HNil

  /** Short hand for 8 coordinates. */
  type Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8] = Value[T1] ::
    Value[T2] ::
    Value[T3] ::
    Value[T4] ::
    Value[T5] ::
    Value[T6] ::
    Value[T7] ::
    Value[T8] ::
    HNil

  /** Short hand for 9 coordinates. */
  type Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9] = Value[T1] ::
    Value[T2] ::
    Value[T3] ::
    Value[T4] ::
    Value[T5] ::
    Value[T6] ::
    Value[T7] ::
    Value[T8] ::
    Value[T9] ::
    HNil
}
