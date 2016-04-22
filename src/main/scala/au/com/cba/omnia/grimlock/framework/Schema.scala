// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.framework.content.metadata

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.encoding._

import java.util.Date

import scala.reflect._

/** Base trait for variable schemas. */
trait Schema {
  /** Type of the schema's data. */
  type S

  /** The type of variable. */
  val kind: Type

  /**
   * Validates if a value confirms to this schema.
   *
   * @param value The value to validate.
   *
   * @return True is the value confirms to this schema, false otherwise.
   */
  def validate(value: Value { type V = S }): Boolean

  /**
   * Return a consise (terse) string representation of a schema.
   *
   * @param codec The codec used to encode this schema's data.
   */
  def toShortString(codec: Codec { type C = S }): String = {
    kind.toShortString + round(paramString(true, s => codec.encode(s)))
  }

  override def toString(): String = {
    this.getClass.getSimpleName + square(typeString()) + "(" + paramString(false, s => s.toString) + ")"
  }

  protected def paramString(short: Boolean, f: (S) => String): String = ""
  protected def typeString(): String = ""

  private def round(str: String): String = if (str.isEmpty) str else "(" + str + ")"
  private def square(str: String): String = if (str.isEmpty) str else "[" + str + "]"
}

/** Companion object to the `Schema` trait. */
object Schema {
  /**
   * Parse a schema from a string.
   *
   * @param str String from which to parse the schema.
   * @param cdc Codec with which to decode the data.
   *
   * @return A `Some[Schema]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String, cdc: Codec): Option[Schema { type S = cdc.C }] = {
    str match {
      case ContinuousSchema.Pattern(_) => ContinuousSchema.fromShortString(str, cdc)
      case DiscreteSchema.Pattern(_, _) => DiscreteSchema.fromShortString(str, cdc)
      case NominalSchema.Pattern(_) => NominalSchema.fromShortString(str, cdc)
      case OrdinalSchema.Pattern(_) => OrdinalSchema.fromShortString(str, cdc)
      case DateSchema.Pattern(_) => DateSchema.fromShortString(str, cdc)
      case _ => None
    }
  }
}

/** Trait for dealing with schema parameters. */
private[metadata] trait SchemaParameters {
  protected def parse(codec: Codec, value: String): Option[codec.C] = codec.decode(value).map(_.value)

  protected def splitRange(range: String): Option[(String, String)] = {
    range.split(":") match {
      case Array(lower, upper) => Some((lower, upper))
      case _ => None
    }
  }

  protected def splitSet(set: String): Set[String] = set.split("(?<!\\\\),").toSet

  protected def writeRange[S](short: Boolean, range: Option[(S, S)], f: (S) => String): String = {
    range.map { case (lower, upper) => f(lower) + (if (short) { ":" } else { "," }) + f(upper) }.getOrElse("")
  }

  protected def writeSet[S](short: Boolean, set: Set[S], f: (S) => String): String = {
    writeList(short, set.toList, f, "Set")
  }

  protected def writeOrderedSet[S : Ordering](short: Boolean, set: Set[S], f: (S) => String): String = {
    writeList(short, set.toList.sorted, f, "Set")
  }

  private def writeList[S](short: Boolean, list: List[S], f: (S) => String, name: String): String = {
    if (list.isEmpty) {
      ""
    } else {
      val args = list.map(f(_).replaceAll(",", "\\\\,")).mkString(",")

      if (short) {
        args
      } else {
        name + "(" + args + ")"
      }
    }
  }
}

/** Base trait for schemas for numerical variables. */
trait NumericalSchema[T] extends Schema {
  type S = T

  /* Optional range of variable */
  val range: Option[(T, T)]

  protected def validateRange(value: Value { type V = S })(implicit ev: Numeric[S]): Boolean = {
    import ev._

    range.map { case (lower, upper) => value.value >= lower && value.value <= upper }.getOrElse(true)
  }
}

/**
 * Schema for continuous variables.
 *
 * @param range The optional range of the variable.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class ContinuousSchema[T: ClassTag] private (range: Option[(T, T)])(
  implicit ev: Numeric[T]) extends NumericalSchema[T] with SchemaParameters {
  val kind = Type.Continuous

  def validate(value: Value { type V = S }): Boolean = validateRange(value)

  override protected def paramString(short: Boolean, f: (S) => String): String = writeRange(short, range, f)
  override protected def typeString(): String = classTag[T].runtimeClass.getName.capitalize
}

/** Companion object to `ContinuousSchema` case class. */
object ContinuousSchema extends SchemaParameters {
  /** Pattern for matching short string continuous schema. */
  val Pattern = (Type.Continuous.name + """(?:\((?:(.*?:.*))?\))?""").r

  /** Construct a continuous schema with unbounded range. */
  def apply[T: ClassTag]()(implicit ev: Numeric[T]): ContinuousSchema[T] = ContinuousSchema(None)

  /**
   * Construct a continuous schema with bounded range.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   */
  def apply[T: ClassTag](lower: T, upper: T)(implicit ev: Numeric[T]): ContinuousSchema[T] = {
    ContinuousSchema(Some((lower, upper)))
  }

  /**
   * Parse a continuous schema from string.
   *
   * @param str The string to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[ContinuousSchema]` if successful, `None` otherwise.
   */
  def fromShortString(str: String, cdc: Codec): Option[ContinuousSchema[cdc.C]] = {
    (cdc.tag, cdc.numeric, str) match {
      case (Some(tag), Some(ev), Pattern(null)) => Some(ContinuousSchema()(tag, ev))
      case (_, _, Pattern(range)) => splitRange(range).flatMap { case (min, max) => fromComponents(min, max, cdc) }
      case _ => None
    }
  }

  /**
   * Parse a continuous schema from components.
   *
   * @param min The minimum value string to parse.
   * @param max The maximum value string to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[ContinuousSchema]` if successful, `None` otherwise.
   */
  def fromComponents(min: String, max: String, cdc: Codec): Option[ContinuousSchema[cdc.C]] = {
    for {
      low <- parse(cdc, min)
      upp <- parse(cdc, max)
      tag <- cdc.tag
      ev <- cdc.numeric
    } yield ContinuousSchema(low, upp)(tag, ev)
  }
}

/**
 * Schema for discrete variables.
 *
 * @param range The optional range of the variable
 * @param step  The optional step size.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class DiscreteSchema[T: ClassTag] private (range: Option[(T, T)], step: Option[T])(
  implicit ev: Integral[T]) extends NumericalSchema[T] with SchemaParameters {
  val kind = Type.Discrete

  def validate(value: Value { type V = S }): Boolean = {
    import ev._

    validateRange(value) && step.map { case s => (value.value % s) == 0 }.getOrElse(true)
  }

  override protected def paramString(short: Boolean, f: (S) => String): String = {
    step.map { s => writeRange(short, range, f) + "," + f(s) }.getOrElse("")
  }
  override protected def typeString(): String = classTag[T].runtimeClass.getName.capitalize
}

/** Companion object to `DiscreteSchema` case class. */
object DiscreteSchema extends SchemaParameters {
  /** Pattern for matching short string discrete schema. */
  val Pattern = (Type.Discrete.name + """(?:\((?:(.*?:.*?),(.*))?\))?""").r

  /** Construct a discrete schema with unbounded range and step size 1. */
  def apply[T: ClassTag]()(implicit ev: Integral[T]): DiscreteSchema[T] = DiscreteSchema(None, None)

  /**
   * Construct a discrete schema with bounded range and step size.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   * @param step  The step size.
   */
  def apply[T: ClassTag](lower: T, upper: T, step: T)(implicit ev: Integral[T]): DiscreteSchema[T] = {
    DiscreteSchema(Some((lower, upper)), Some(step))
  }

  /**
   * Parse a discrete schema from string.
   *
   * @param str The string to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromShortString(str: String, cdc: Codec): Option[DiscreteSchema[cdc.C]] = {
    (cdc.tag, cdc.integral, str) match {
      case (Some(tag), Some(ev), Pattern(null, null)) => Some(DiscreteSchema()(tag, ev))
      case (_, _, Pattern(range, step)) =>
        splitRange(range).flatMap { case (min, max) => fromComponents(min, max, step, cdc) }
      case _ => None
    }
  }

  /**
   * Parse a discrete schema from components.
   *
   * @param min  The minimum value string to parse.
   * @param max  The maximum value string to parse.
   * @param step The step size string to parse.
   * @param cdc  The codec to parse with.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromComponents(min: String, max: String, step: String, cdc: Codec): Option[DiscreteSchema[cdc.C]] = {
    for {
      low <- parse(cdc, min)
      upp <- parse(cdc, max)
      stp <- parse(cdc, step)
      tag <- cdc.tag
      ev <- cdc.integral
    } yield DiscreteSchema(low, upp, stp)(tag, ev)
  }
}

/** Base trait for schemas for categorical variables. */
trait CategoricalSchema[T] extends Schema with SchemaParameters {
  type S = T

  /** Values the variable can take. */
  val domain: Set[S]

  def validate(value: Value { type V = S }): Boolean = domain.isEmpty || domain.contains(value.value)

  protected def shortName(name: String): String = {
    if (name.contains(".")) { if (name == "java.lang.String") { "String" } else { name } } else { name.capitalize }
  }
}

/**
 * Schema for nominal variables.
 *
 * @param domain The values of the variable.
 */
case class NominalSchema[T: ClassTag](domain: Set[T] = Set[T]()) extends CategoricalSchema[T] {
  val kind = Type.Nominal

  override protected def paramString(short: Boolean, f: (S) => String): String = writeSet(short, domain, f)
  override protected def typeString(): String = shortName(classTag[T].runtimeClass.getName)
}

/** Companion object to `NominalSchema` case class. */
object NominalSchema extends SchemaParameters {
  /** Pattern for matching short string nominal schema. */
  val Pattern = (Type.Nominal.name + """(?:\((?:(.*?))?\))?""").r

  /**
   * Parse a nominal schema from string.
   *
   * @param str The string to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[NominalSchema]` if successful, `None` otherwise.
   */
  def fromShortString(str: String, cdc: Codec): Option[NominalSchema[cdc.C]] = {
    (cdc.tag, str) match {
      case (Some(tag), Pattern(null)) => Some(NominalSchema()(tag))
      case (Some(tag), Pattern("")) => Some(NominalSchema()(tag))
      case (_, Pattern(domain)) => fromComponents(splitSet(domain), cdc)
      case _ => None
    }
  }

  /**
   * Parse a nominal schema from string components.
   *
   * @param dom The domain value strings to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[NominalSchema]` if successful, `None` otherwise.
   */
  def fromComponents(dom: Set[String], cdc: Codec): Option[NominalSchema[cdc.C]] = {
    val values = dom.map(parse(cdc, _))

    (cdc.tag, values.isEmpty || values.exists(_.isEmpty)) match {
      case (Some(tag), false) => Some(NominalSchema(values.map(_.get))(tag))
      case _ => None
    }
  }
}

/**
 * Schema for ordinal variables.
 *
 * @param domain The optional values of the variable.
 */
case class OrdinalSchema[T: ClassTag: Ordering](domain: Set[T] = Set[T]()) extends CategoricalSchema[T] {
  val kind = Type.Ordinal

  override protected def paramString(short: Boolean, f: (S) => String): String = writeOrderedSet(short, domain, f)
  override protected def typeString(): String = shortName(classTag[T].runtimeClass.getName)
}

/** Companion object to `OrdinalSchema`. */
object OrdinalSchema extends SchemaParameters {
  /** Pattern for matching short string ordinal schema. */
  val Pattern = (Type.Ordinal.name + """(?:\((?:(.*?))?\))?""").r

  /**
   * Parse a ordinal schema from string.
   *
   * @param str The string to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[OrdinalSchema]` if successful, `None` otherwise.
   */
  def fromShortString(str: String, cdc: Codec): Option[OrdinalSchema[cdc.C]] = {
    (cdc.tag, cdc.ordering, str) match {
      case (Some(tag), Some(ev), Pattern(null)) => Some(OrdinalSchema()(tag, ev))
      case (Some(tag), Some(ev), Pattern("")) => Some(OrdinalSchema()(tag, ev))
      case (_, _, Pattern(domain)) => fromComponents(splitSet(domain), cdc)
      case _ => None
    }
  }

  /**
   * Parse a ordinal schema from string components.
   *
   * @param dom The domain value strings to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[OrdinalSchema]` if successful, `None` otherwise.
   */
  def fromComponents(dom: Set[String], cdc: Codec): Option[OrdinalSchema[cdc.C]] = {
    val values = dom.map(parse(cdc, _))

    (cdc.tag, cdc.ordering, values.isEmpty || values.exists(_.isEmpty)) match {
      case (Some(tag), Some(ev), false) => Some(OrdinalSchema(values.map(_.get))(tag, ev))
      case _ => None
    }
  }
}

/**
 * Schema for date variables.
 *
 * @param dates The optional values of the variable.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class DateSchema[T: ClassTag] private (dates: Option[Either[(T, T), Set[T]]])(
  implicit ev: T => Date) extends Schema with SchemaParameters {
  type S = T

  val kind = Type.Date

  def validate(value: Value { type V = S }): Boolean = {
    dates match {
      case None => true
      case Some(Left((lower, upper))) => (lower.compareTo(value.value) <= 0) && (upper.compareTo(value.value) >= 0)
      case Some(Right(domain)) => domain.contains(value.value)
    }
  }

  override protected def paramString(short: Boolean, f: (S) => String): String = {
    dates match {
      case None => ""
      case Some(Left(range)) => writeRange(short, Some(range), f)
      case Some(Right(domain)) => writeSet(short, domain, f)
    }
  }
  override protected def typeString(): String = classTag[T].runtimeClass.getName
}

/** Companion object to `DateSchema`. */
object DateSchema extends SchemaParameters {
  /** Pattern for matching short string date schema. */
  val Pattern = (Type.Date.name + """(?:\((?:(.*?))?\))?""").r

  /** Construct an unbounded date schema. */
  def apply[T: ClassTag]()(implicit ev: T => Date): DateSchema[T] = DateSchema[T](None)

  /**
   * Construct a date schema with bounded range.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   */
  def apply[T: ClassTag](lower: T, upper: T)(implicit ev: T => Date): DateSchema[T] = {
    DateSchema[T](Some(Left((lower, upper))))
  }

  /**
   * Construct a date schema with a set of valid dates.
   *
   * @param domain The set of legal values.
   */
  def apply[T: ClassTag](domain: Set[T])(implicit ev: T => Date): DateSchema[T] = DateSchema[T](Some(Right(domain)))

  /**
   * Parse a date schema from string.
   *
   * @param str The string to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromShortString(str: String, cdc: Codec): Option[DateSchema[cdc.C]] = {
    (cdc.tag, cdc.date, str) match {
      case (Some(tag), Some(ev), Pattern(null)) => Some(DateSchema()(tag, ev))
      case (Some(tag), Some(ev), Pattern("")) => Some(DateSchema()(tag, ev))
      case (_, _, RangePattern(range)) =>
        splitRange(range).flatMap { case (lower, upper) => fromComponents(lower, upper, cdc) }
      case (_, _, Pattern(domain)) => fromComponents(splitSet(domain), cdc)
      case _ => None
    }
  }

  /**
   * Parse a date schema from components.
   *
   * @param min The minimum value string to parse.
   * @param max The maximum value string to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromComponents(min: String, max: String, cdc: Codec): Option[DateSchema[cdc.C]] = {
    for {
      low <- parse(cdc, min)
      upp <- parse(cdc, max)
      tag <- cdc.tag
      ev <- cdc.date
    } yield DateSchema(low, upp)(tag, ev)
  }

  /**
   * Parse a date schema from string components.
   *
   * @param dom The domain value strings to parse.
   * @param cdc The codec to parse with.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromComponents(dom: Set[String], cdc: Codec): Option[DateSchema[cdc.C]] = {
    val values = dom.map(parse(cdc, _))

    (cdc.tag, cdc.date, values.isEmpty || values.exists(_.isEmpty)) match {
      case (Some(tag), Some(ev), false) => Some(DateSchema(values.map(_.get))(tag, ev))
      case _ => None
    }
  }

  private val RangePattern = (Type.Date.name + """(?:\((?:(.*?:.*))?\))?""").r
}

/** Schema for structured data variables. */
trait StructuredSchema extends Schema

