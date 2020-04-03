// Copyright 2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

trait TestMaterialise extends TestGrimlock {
  val data = List(
    ("a", "one", Content(ContinuousSchema[Double](), 3.14)),
    ("a", "two", Content(NominalSchema[String](), "foo")),
    ("a", "three", Content(DiscreteSchema[Long](), 42L)),
    ("b", "one", Content(ContinuousSchema[Double](), 6.28)),
    ("b", "two", Content(DiscreteSchema[Long](), 123L)),
    ("b", "three", Content(ContinuousSchema[Double](), 9.42)),
    ("c", "two", Content(NominalSchema[String](), "bar")),
    ("c", "three", Content(ContinuousSchema[Double](), 12.56))
  )

  val result = List(
    Cell(Position("a", "one"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("a", "two"), Content(NominalSchema[String](), "foo")),
    Cell(Position("a", "three"), Content(DiscreteSchema[Long](), 42L)),
    Cell(Position("b", "one"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("b", "two"), Content(DiscreteSchema[Long](), 123L)),
    Cell(Position("b", "three"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("c", "two"), Content(NominalSchema[String](), "bar")),
    Cell(Position("c", "three"), Content(ContinuousSchema[Double](), 12.56))
  )
}

class TestScalaMaterialise extends TestMaterialise with TestScala {
  import ctx.implicits.matrix.tuple2ToMatrix

  "NativeOperations.materialise" should "return its list" in {
    ctx.materialise(data.data).sortBy(_.position) shouldBe result.sortBy(_.position)
  }
}

class TestScaldingMaterialise extends TestMaterialise with TestScalding {
  import ctx.implicits.matrix.tuple2ToMatrix

  "NativeOperations.materialise" should "return its list" in {
    ctx.materialise(data.data).sortBy(_.position) shouldBe result.sortBy(_.position)
  }
}

class TestSparkMaterialise extends TestMaterialise with TestSpark {
  import ctx.implicits.matrix.tuple2ToMatrix

  "NativeOperations.materialise" should "return its list" in {
    ctx.materialise(data.data).sortBy(_.position) shouldBe result.sortBy(_.position)
  }
}

