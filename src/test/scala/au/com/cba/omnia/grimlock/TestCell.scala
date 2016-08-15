// Copyright 2015,2016 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

class TestCell extends TestGrimlock {

  "A Cell" should "return its string" in {
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Double](), 3.14)).toString() shouldBe
      "Cell(Position2D(StringValue(foo,StringCodec),LongValue(123,LongCodec)),Content(ContinuousSchema[Double](),DoubleValue(3.14,DoubleCodec)))"
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Double](), 3.14)).toShortString(".", true, true) shouldBe
      "foo.123.double.continuous.3.14"
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Double](), 3.14)).toShortString(".", false, true) shouldBe
      "foo.123.continuous.3.14"
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Double](), 3.14)).toShortString(".", true, false) shouldBe
      "foo.123.double.3.14"
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Double](), 3.14)).toShortString(".", false, false) shouldBe
      "foo.123.3.14"
  }

  "A Cell" should "relocate" in {
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Double](), 3.14))
      .relocate(_.position.append("abc")) shouldBe
        (Cell(Position3D("foo", 123, "abc"), Content(ContinuousSchema[Double](), 3.14)))
  }

  "A Cell" should "mutate" in {
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Double](), 3.14))
      .mutate(_ => Content(DiscreteSchema[Long](), 42)) shouldBe
        (Cell(Position2D("foo", 123), Content(DiscreteSchema[Long](), 42)))
  }

  val schema = Content.parser(DoubleCodec, ContinuousSchema[Double]())
  val dictionary = Map("123" -> schema)

  "A Cell" should "parse 1D" in {
    Cell.parse1D(":", LongCodec)("123:double:continuous:3.14") shouldBe
      Some(Right(Cell(Position1D(123), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse1D(":", LongCodec)("abc:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'abc:double:continuous:3.14'"))
    Cell.parse1D(":", LongCodec)("123:double:continuous:abc") shouldBe
      Some(Left("Unable to decode: '123:double:continuous:abc'"))
    Cell.parse1D(":", LongCodec)("123:double:continuous:3:14") shouldBe
      Some(Left("Unable to decode: '123:double:continuous:3:14'"))
    Cell.parse1D(":", LongCodec)("123:double|continuous:3.14") shouldBe
      Some(Left("Unable to split: '123:double|continuous:3.14'"))
  }

  "A Cell" should "parse 1D with dictionary" in {
    Cell.parse1DWithDictionary(dictionary, ":", LongCodec)("123:3.14") shouldBe
      Some(Right(Cell(Position1D(123), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse1DWithDictionary(dictionary, ":", LongCodec)("abc:3.14") shouldBe
      Some(Left("Missing schema for: 'abc:3.14'"))
    Cell.parse1DWithDictionary(dictionary, ":", LongCodec)("123:abc") shouldBe
      Some(Left("Unable to decode: '123:abc'"))
    Cell.parse1DWithDictionary(dictionary, ":", LongCodec)("123:3:14") shouldBe
      Some(Left("Unable to decode: '123:3:14'"))
    Cell.parse1DWithDictionary(dictionary, ":", LongCodec)("123|3.14") shouldBe
      Some(Left("Unable to split: '123|3.14'"))
  }

  "A Cell" should "parse 1D with schema" in {
    Cell.parse1DWithSchema(schema, ":", LongCodec)("123:3.14") shouldBe
      Some(Right(Cell(Position1D(123), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse1DWithSchema(schema, ":", LongCodec)("abc:3.14") shouldBe
      Some(Left("Unable to decode: 'abc:3.14'"))
    Cell.parse1DWithSchema(schema, ":", LongCodec)("123:abc") shouldBe
      Some(Left("Unable to decode: '123:abc'"))
    Cell.parse1DWithSchema(schema, ":", LongCodec)("123:3:14") shouldBe
      Some(Left("Unable to decode: '123:3:14'"))
    Cell.parse1DWithSchema(schema, ":", LongCodec)("123|3.14") shouldBe
      Some(Left("Unable to split: '123|3.14'"))
  }

  "A Cell" should "parse 2D" in {
    Cell.parse2D(":", LongCodec, StringCodec)("123:def:double:continuous:3.14") shouldBe
      Some(Right(Cell(Position2D(123, "def"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse2D(":", LongCodec, StringCodec)("abc:def:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'abc:def:double:continuous:3.14'"))
    Cell.parse2D(":", StringCodec, LongCodec)("abc:def:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'abc:def:double:continuous:3.14'"))
    Cell.parse2D(":", LongCodec, StringCodec)("123:def:double:continuous:abc") shouldBe
      Some(Left("Unable to decode: '123:def:double:continuous:abc'"))
    Cell.parse2D(":", LongCodec, StringCodec)("123:def:double:continuous:3:14") shouldBe
      Some(Left("Unable to decode: '123:def:double:continuous:3:14'"))
    Cell.parse2D(":", LongCodec, StringCodec)("123:def:double|continuous:3.14") shouldBe
      Some(Left("Unable to split: '123:def:double|continuous:3.14'"))
  }

  "A Cell" should "parse 2D with dictionary" in {
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodec, StringCodec)("123:def:3.14") shouldBe
      Some(Right(Cell(Position2D(123, "def"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse2DWithDictionary(dictionary, Second, ":", StringCodec, LongCodec)("def:123:3.14") shouldBe
      Some(Right(Cell(Position2D("def", 123), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodec, StringCodec)("abc:def:3.14") shouldBe
      Some(Left("Missing schema for: 'abc:def:3.14'"))
    Cell.parse2DWithDictionary(dictionary, Second, ":", LongCodec, StringCodec)("abc:def:3.14") shouldBe
      Some(Left("Missing schema for: 'abc:def:3.14'"))
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodec, StringCodec)("123:def:abc") shouldBe
      Some(Left("Unable to decode: '123:def:abc'"))
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodec, StringCodec)("123:def:3:14") shouldBe
      Some(Left("Unable to decode: '123:def:3:14'"))
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodec, StringCodec)("123|def:3.14") shouldBe
      Some(Left("Unable to split: '123|def:3.14'"))
  }

  "A Cell" should "parse 2D with schema" in {
    Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)("123:def:3.14") shouldBe
      Some(Right(Cell(Position2D(123, "def"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)("abc:def:3.14") shouldBe
      Some(Left("Unable to decode: 'abc:def:3.14'"))
    Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)("123:def:abc") shouldBe
      Some(Left("Unable to decode: '123:def:abc'"))
    Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)("123:def:3:14") shouldBe
      Some(Left("Unable to decode: '123:def:3:14'"))
    Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)("123:def|3.14") shouldBe
      Some(Left("Unable to split: '123:def|3.14'"))
  }

  "A Cell" should "parse 3D" in {
    Cell.parse3D(":", LongCodec, StringCodec, StringCodec)("123:def:ghi:double:continuous:3.14") shouldBe
      Some(Right(Cell(Position3D(123, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse3D(":", LongCodec, StringCodec, StringCodec)("abc:def:ghi:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'abc:def:ghi:double:continuous:3.14'"))
    Cell.parse3D(":", StringCodec, LongCodec, StringCodec)("def:abc:ghi:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'def:abc:ghi:double:continuous:3.14'"))
    Cell.parse3D(":", StringCodec, StringCodec, LongCodec)("def:ghi:abc:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'def:ghi:abc:double:continuous:3.14'"))
    Cell.parse3D(":", LongCodec, StringCodec, StringCodec)("123:def:ghi:double:continuous:abc") shouldBe
      Some(Left("Unable to decode: '123:def:ghi:double:continuous:abc'"))
    Cell.parse3D(":", LongCodec, StringCodec, StringCodec)("123:def:ghi:double:continuous:3:14") shouldBe
      Some(Left("Unable to decode: '123:def:ghi:double:continuous:3:14'"))
    Cell.parse3D(":", LongCodec, StringCodec, StringCodec)("123:def:ghi:double|continuous:3.14") shouldBe
      Some(Left("Unable to split: '123:def:ghi:double|continuous:3.14'"))
  }

  "A Cell" should "parse 3D with dictionary" in {
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec)(
      "123:def:ghi:3.14") shouldBe
        Some(Right(Cell(Position3D(123, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse3DWithDictionary(dictionary, Second, ":", StringCodec, LongCodec, StringCodec)(
      "def:123:ghi:3.14") shouldBe
        Some(Right(Cell(Position3D("def", 123, "ghi"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse3DWithDictionary(dictionary, Third, ":", StringCodec, StringCodec, LongCodec)(
      "def:ghi:123:3.14") shouldBe
        Some(Right(Cell(Position3D("def", "ghi", 123), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec)(
      "abc:def:ghi:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:3.14'"))
    Cell.parse3DWithDictionary(dictionary, Second, ":", LongCodec, StringCodec, StringCodec)(
      "abc:def:ghi:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:3.14'"))
    Cell.parse3DWithDictionary(dictionary, Third, ":", LongCodec, StringCodec, StringCodec)(
      "abc:def:ghi:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:3.14'"))
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec)(
      "123:def:ghi:abc") shouldBe Some(Left("Unable to decode: '123:def:ghi:abc'"))
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec)(
      "123:def:ghi:3:14") shouldBe Some(Left("Unable to decode: '123:def:ghi:3:14'"))
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec)(
      "123|def:ghi:3.14") shouldBe Some(Left("Unable to split: '123|def:ghi:3.14'"))
  }

  "A Cell" should "parse 3D with schema" in {
    Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)("123:def:ghi:3.14") shouldBe
      Some(Right(Cell(Position3D(123, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)("abc:def:ghi:3.14") shouldBe
      Some(Left("Unable to decode: 'abc:def:ghi:3.14'"))
    Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)("123:def:ghi:abc") shouldBe
      Some(Left("Unable to decode: '123:def:ghi:abc'"))
    Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)("123:def:ghi:3:14") shouldBe
      Some(Left("Unable to decode: '123:def:ghi:3:14'"))
    Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)("123:def|ghi:3.14") shouldBe
      Some(Left("Unable to split: '123:def|ghi:3.14'"))
  }

  "A Cell" should "parse 4D" in {
    Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:double:continuous:3.14") shouldBe
        Some(Right(Cell(Position4D(123, "def", "ghi", "klm"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:double:continuous:3.14") shouldBe
        Some(Left("Unable to decode: 'abc:def:ghi:klm:double:continuous:3.14'"))
    Cell.parse4D(":", StringCodec, LongCodec, StringCodec, StringCodec)(
      "def:abc:ghi:klm:double:continuous:3.14") shouldBe
        Some(Left("Unable to decode: 'def:abc:ghi:klm:double:continuous:3.14'"))
    Cell.parse4D(":", StringCodec, StringCodec, LongCodec, StringCodec)(
      "def:ghi:abc:klm:double:continuous:3.14") shouldBe
        Some(Left("Unable to decode: 'def:ghi:abc:klm:double:continuous:3.14'"))
    Cell.parse4D(":", StringCodec, StringCodec, StringCodec, LongCodec)(
      "def:ghi:klm:abc:double:continuous:3.14") shouldBe
        Some(Left("Unable to decode: 'def:ghi:klm:abc:double:continuous:3.14'"))
    Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:double:continuous:abc") shouldBe
        Some(Left("Unable to decode: '123:def:ghi:klm:double:continuous:abc'"))
    Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:double:continuous:3:14") shouldBe
        Some(Left("Unable to decode: '123:def:ghi:klm:double:continuous:3:14'"))
    Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:double|continuous:3.14") shouldBe
        Some(Left("Unable to split: '123:def:ghi:klm:double|continuous:3.14'"))
  }

  "A Cell" should "parse 4D with dictionary" in {
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:3.14") shouldBe
        Some(Right(Cell(Position4D(123, "def", "ghi", "klm"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse4DWithDictionary(dictionary, Second, ":", StringCodec, LongCodec, StringCodec, StringCodec)(
      "def:123:ghi:klm:3.14") shouldBe
        Some(Right(Cell(Position4D("def", 123, "ghi", "klm"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse4DWithDictionary(dictionary, Third, ":", StringCodec, StringCodec, LongCodec, StringCodec)(
      "def:ghi:123:klm:3.14") shouldBe
        Some(Right(Cell(Position4D("def", "ghi", 123, "klm"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse4DWithDictionary(dictionary, Fourth, ":", StringCodec, StringCodec, StringCodec, LongCodec)(
      "def:ghi:klm:123:3.14") shouldBe
        Some(Right(Cell(Position4D("def", "ghi", "klm", 123), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithDictionary(dictionary, Second, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithDictionary(dictionary, Third, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithDictionary(dictionary, Fourth, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:abc") shouldBe Some(Left("Unable to decode: '123:def:ghi:klm:abc'"))
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:3:14") shouldBe Some(Left("Unable to decode: '123:def:ghi:klm:3:14'"))
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123|def:ghi:klm:3.14") shouldBe Some(Left("Unable to split: '123|def:ghi:klm:3.14'"))
  }

  "A Cell" should "parse 4D with schema" in {
    Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:3.14") shouldBe
        Some(Right(Cell(Position4D(123, "def", "ghi", "klm"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:3.14") shouldBe
        Some(Left("Unable to decode: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:abc") shouldBe
        Some(Left("Unable to decode: '123:def:ghi:klm:abc'"))
    Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:3:14") shouldBe
        Some(Left("Unable to decode: '123:def:ghi:klm:3:14'"))
    Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)(
      "123:def|ghi:klm:3.14") shouldBe
        Some(Left("Unable to split: '123:def|ghi:klm:3.14'"))
  }

  "A Cell" should "parse 5D" in {
    Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:double:continuous:3.14") shouldBe
        Some(Right(Cell(Position5D(123, "def", "ghi", "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:double:continuous:3.14") shouldBe
        Some(Left("Unable to decode: 'abc:def:ghi:klm:xyz:double:continuous:3.14'"))
    Cell.parse5D(":", StringCodec, LongCodec, StringCodec, StringCodec, StringCodec)(
      "def:abc:ghi:klm:xyz:double:continuous:3.14") shouldBe
        Some(Left("Unable to decode: 'def:abc:ghi:klm:xyz:double:continuous:3.14'"))
    Cell.parse5D(":", StringCodec, StringCodec, LongCodec, StringCodec, StringCodec)(
      "def:ghi:abc:klm:xyz:double:continuous:3.14") shouldBe
        Some(Left("Unable to decode: 'def:ghi:abc:klm:xyz:double:continuous:3.14'"))
    Cell.parse5D(":", StringCodec, StringCodec, StringCodec, LongCodec, StringCodec)(
      "def:ghi:klm:abc:xyz:double:continuous:3.14") shouldBe
        Some(Left("Unable to decode: 'def:ghi:klm:abc:xyz:double:continuous:3.14'"))
    Cell.parse5D(":", StringCodec, StringCodec, StringCodec, StringCodec, LongCodec)(
      "def:ghi:klm:xyz:abc:double:continuous:3.14") shouldBe
        Some(Left("Unable to decode: 'def:ghi:klm:xyz:abc:double:continuous:3.14'"))
    Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:double:continuous:abc") shouldBe
        Some(Left("Unable to decode: '123:def:ghi:klm:xyz:double:continuous:abc'"))
    Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:double:continuous:3:14") shouldBe
        Some(Left("Unable to decode: '123:def:ghi:klm:xyz:double:continuous:3:14'"))
    Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:double|continuous:3.14") shouldBe
        Some(Left("Unable to split: '123:def:ghi:klm:xyz:double|continuous:3.14'"))
  }

  "A Cell" should "parse 5D with dictionary" in {
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:3.14") shouldBe
        Some(Right(Cell(Position5D(123, "def", "ghi", "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse5DWithDictionary(dictionary, Second, ":", StringCodec, LongCodec, StringCodec, StringCodec, StringCodec)(
      "def:123:ghi:klm:xyz:3.14") shouldBe
        Some(Right(Cell(Position5D("def", 123, "ghi", "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse5DWithDictionary(dictionary, Third, ":", StringCodec, StringCodec, LongCodec, StringCodec, StringCodec)(
      "def:ghi:123:klm:xyz:3.14") shouldBe
        Some(Right(Cell(Position5D("def", "ghi", 123, "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse5DWithDictionary(dictionary, Fourth, ":", StringCodec, StringCodec, StringCodec, LongCodec, StringCodec)(
      "def:ghi:klm:123:xyz:3.14") shouldBe
        Some(Right(Cell(Position5D("def", "ghi", "klm", 123, "xyz"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse5DWithDictionary(dictionary, Fifth, ":", StringCodec, StringCodec, StringCodec, StringCodec, LongCodec)(
      "def:ghi:klm:xyz:123:3.14") shouldBe
        Some(Right(Cell(Position5D("def", "ghi", "klm", "xyz", 123), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, Second, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, Third, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, Fourth, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, Fifth, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:abc") shouldBe Some(Left("Unable to decode: '123:def:ghi:klm:xyz:abc'"))
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:3:14") shouldBe Some(Left("Unable to decode: '123:def:ghi:klm:xyz:3:14'"))
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123|def:ghi:klm:xyz:3.14") shouldBe Some(Left("Unable to split: '123|def:ghi:klm:xyz:3.14'"))
  }

  "A Cell" should "parse 5D with schema" in {
    Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:3.14") shouldBe
        Some(Right(Cell(Position5D(123, "def", "ghi", "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe
        Some(Left("Unable to decode: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:abc") shouldBe
        Some(Left("Unable to decode: '123:def:ghi:klm:xyz:abc'"))
    Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:3:14") shouldBe
        Some(Left("Unable to decode: '123:def:ghi:klm:xyz:3:14'"))
    Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def|ghi:klm:xyz:3.14") shouldBe
        Some(Left("Unable to split: '123:def|ghi:klm:xyz:3.14'"))
  }

  "A Cell" should "parse 6D" in {
    Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:uvw:double:continuous:3.14") shouldBe
      Some(Right(Cell(Position6D(123, "def", "ghi", "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:uvw:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'abc:def:ghi:klm:xyz:uvw:double:continuous:3.14'"))
    Cell.parse6D(":", StringCodec, LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "def:abc:ghi:klm:xyz:uvw:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'def:abc:ghi:klm:xyz:uvw:double:continuous:3.14'"))
    Cell.parse6D(":", StringCodec, StringCodec, LongCodec, StringCodec, StringCodec, StringCodec)(
      "def:ghi:abc:klm:xyz:uvw:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'def:ghi:abc:klm:xyz:uvw:double:continuous:3.14'"))
    Cell.parse6D(":", StringCodec, StringCodec, StringCodec, LongCodec, StringCodec, StringCodec)(
      "def:ghi:klm:abc:xyz:uvw:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'def:ghi:klm:abc:xyz:uvw:double:continuous:3.14'"))
    Cell.parse6D(":", StringCodec, StringCodec, StringCodec, StringCodec, LongCodec, StringCodec)(
      "def:ghi:klm:xyz:abc:uvw:double:continuous:3.14") shouldBe
      Some(Left("Unable to decode: 'def:ghi:klm:xyz:abc:uvw:double:continuous:3.14'"))
    Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:uvw:double:continuous:abc") shouldBe
      Some(Left("Unable to decode: '123:def:ghi:klm:xyz:uvw:double:continuous:abc'"))
    Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:uvw:double:continuous:3:14") shouldBe
      Some(Left("Unable to decode: '123:def:ghi:klm:xyz:uvw:double:continuous:3:14'"))
    Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:uvw:double|continuous:3.14") shouldBe
      Some(Left("Unable to split: '123:def:ghi:klm:xyz:uvw:double|continuous:3.14'"))
  }

  "A Cell" should "parse 6D with dictionary" in {
    Cell.parse6DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:uvw:3.14") shouldBe
      Some(Right(Cell(Position6D(123, "def", "ghi", "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse6DWithDictionary(dictionary, Second, ":", StringCodec, LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "def:123:ghi:klm:xyz:uvw:3.14") shouldBe
      Some(Right(Cell(Position6D("def", 123, "ghi", "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse6DWithDictionary(dictionary, Third, ":", StringCodec, StringCodec, LongCodec, StringCodec, StringCodec, StringCodec)(
      "def:ghi:123:klm:xyz:uvw:3.14") shouldBe
      Some(Right(Cell(Position6D("def", "ghi", 123, "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse6DWithDictionary(dictionary, Fourth, ":", StringCodec, StringCodec, StringCodec, LongCodec, StringCodec, StringCodec)(
      "def:ghi:klm:123:xyz:uvw:3.14") shouldBe
      Some(Right(Cell(Position6D("def", "ghi", "klm", 123, "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse6DWithDictionary(dictionary, Fifth, ":", StringCodec, StringCodec, StringCodec, StringCodec, LongCodec, StringCodec)(
      "def:ghi:klm:xyz:123:uvw:3.14") shouldBe
      Some(Right(Cell(Position6D("def", "ghi", "klm", "xyz", 123, "uvw"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse6DWithDictionary(dictionary, Sixth, ":", StringCodec, StringCodec, StringCodec, StringCodec, StringCodec, LongCodec)(
      "def:ghi:klm:xyz:uvw:123:3.14") shouldBe
      Some(Right(Cell(Position6D("def", "ghi", "klm", "xyz", "uvw", 123), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse6DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:uvw:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    Cell.parse6DWithDictionary(dictionary, Second, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:uvw:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    Cell.parse6DWithDictionary(dictionary, Third, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:uvw:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    Cell.parse6DWithDictionary(dictionary, Fourth, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:uvw:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    Cell.parse6DWithDictionary(dictionary, Fifth, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:uvw:3.14") shouldBe Some(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    Cell.parse6DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:abc:uvw") shouldBe Some(Left("Unable to decode: '123:def:ghi:klm:xyz:abc:uvw'"))
    Cell.parse6DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:uvw:3:14") shouldBe Some(Left("Unable to decode: '123:def:ghi:klm:xyz:uvw:3:14'"))
    Cell.parse6DWithDictionary(dictionary, First, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123|def:ghi:klm:xyz:uvw:3.14") shouldBe Some(Left("Unable to split: '123|def:ghi:klm:xyz:uvw:3.14'"))
  }

  "A Cell" should "parse 6D with schema" in {
    Cell.parse6DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:uvw:3.14") shouldBe
      Some(Right(Cell(Position6D(123, "def", "ghi", "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14))))
    Cell.parse6DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "abc:def:ghi:klm:xyz:uvw:3.14") shouldBe
      Some(Left("Unable to decode: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    Cell.parse6DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:abc:uvw") shouldBe
      Some(Left("Unable to decode: '123:def:ghi:klm:xyz:abc:uvw'"))
    Cell.parse6DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def:ghi:klm:xyz:uvw:3:14") shouldBe
      Some(Left("Unable to decode: '123:def:ghi:klm:xyz:uvw:3:14'"))
    Cell.parse6DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)(
      "123:def|ghi:klm:xyz:uvw:3.14") shouldBe
      Some(Left("Unable to split: '123:def|ghi:klm:xyz:uvw:3.14'"))
  }

  val columns = List(("abc", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
    ("def", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
    ("ghi", Content.parser(DoubleCodec, ContinuousSchema[Double]())))

  "A Cell" should "parse table" in {
    Cell.parseTable(columns, 0, ":")("3.14:6.28:9.42") shouldBe List(
      Right(Cell(Position2D("3.14", "def"), Content(ContinuousSchema[Double](), 6.28))),
      Right(Cell(Position2D("3.14", "ghi"), Content(ContinuousSchema[Double](), 9.42))))
    Cell.parseTable(columns, 1, ":")("3.14:6.28:9.42") shouldBe List(
      Right(Cell(Position2D("6.28", "abc"), Content(ContinuousSchema[Double](), 3.14))),
      Right(Cell(Position2D("6.28", "ghi"), Content(ContinuousSchema[Double](), 9.42))))
    Cell.parseTable(columns, 2, ":")("3.14:6.28:9.42") shouldBe List(
      Right(Cell(Position2D("9.42", "abc"), Content(ContinuousSchema[Double](), 3.14))),
      Right(Cell(Position2D("9.42", "def"), Content(ContinuousSchema[Double](), 6.28))))
    Cell.parseTable(columns, 0, ":")("3.14:foo:bar") shouldBe List(
      Left("Unable to decode: '3.14:foo:bar'"),
      Left("Unable to decode: '3.14:foo:bar'"))
    Cell.parseTable(columns, 0, ":")("3.14:foo") shouldBe List(Left("Unable to split: '3.14:foo'"))
  }
}

