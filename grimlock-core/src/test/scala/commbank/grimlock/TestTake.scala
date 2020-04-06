// Copyright 2020 Commonwealth Bank of Australia
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

package commbank.grimlock.test

import commbank.grimlock.framework.environment.Context
import commbank.grimlock.scala.environment.{ Context => ScalaContext }
import commbank.grimlock.scalding.environment.{ Context => ScaldingContext }
import commbank.grimlock.spark.environment.{ Context => SparkContext }

trait TestTake[C <: Context[C]] extends TestGrimlock {
  implicit val enc: C#D[(String, String, Int)]

  val ctx: C

  import ctx.implicits.environment.nativeFunctions

  val item1 = ("a", "one", 42)
  val item2 = ("b", "two", 0)
  val item3 = ("c", "three", -1)
  val item4 = ("d", "four", 99)

  val data = List(item1, item2, item3, item4)

  "NativeOperations.take" should "return subset of items" in {
    ctx.materialise(ctx.from(data).take(2)) shouldBe List(item1, item2)
  }

  it should "return all items if `num` greater than or equal to length of list" in {
    ctx.materialise(ctx.from(data).take(4)) shouldBe data
    ctx.materialise(ctx.from(data).take(5)) shouldBe data
  }

  it should "return no items if `num` is less than or equal to zero" in {
    ctx.materialise(ctx.from(data).take(0)) shouldBe List()
    ctx.materialise(ctx.from(data).take(-1)) shouldBe List()
  }
}

class TestScalaTake extends TestScala with TestTake[ScalaContext] {
  implicit val enc = ScalaContext.encoder[(String, String, Int)]
}

class TestScaldingTake extends TestScalding with TestTake[ScaldingContext] {
  implicit val enc = ScaldingContext.encoder[(String, String, Int)]
}

class TestSparkTake extends TestSpark with TestTake[SparkContext] {
  implicit val enc = SparkContext.encoder[(String, String, Int)]
}

