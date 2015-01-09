// Copyright 2014-2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.examples

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.Matrix._
import au.com.cba.omnia.grimlock.Names._
import au.com.cba.omnia.grimlock.partition._
import au.com.cba.omnia.grimlock.partition.Partitions._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.position.Positions._
import au.com.cba.omnia.grimlock.reduce._
import au.com.cba.omnia.grimlock.transform._
import au.com.cba.omnia.grimlock.Types._
import au.com.cba.omnia.grimlock.utility.Miscellaneous.Collection

import com.twitter.scalding._

class BasicOperations(args : Args) extends Job(args) {
  // Read the data. This returns a 2D matrix (instance x feature).
  val data = read2D("exampleInput.txt")

  // Get the number of rows.
  data
    .size(First)
    .persist("./demo/row_size.out")

  // Get all dimensions of the matrix.
  data
    .shape()
    .persist("./demo/matrix_shape.out")

  // Get the column names.
  data
    .names(Over(Second))
    .persist("./demo/column_names.out")

  // Get the type of variables of each column.
  data
    .types(Over(Second), true)
    .persist("./demo/column_types.txt")

  // Transpose the matrix.
  data
    .permute(Second, First)
    .persist("./demo/transposed.out")

  // Construct a simple query
  def simpleQuery(pos: Position, con: Content) = (con.value gtr 995) || (con.value equ "F")

  // Find all co-ordinates that match the above simple query.
  val coords = data
    .which(simpleQuery)
    .persist("./demo/query.txt")

  // Get the data for the above coordinates.
  data
    .get(coords)
    .persist("./demo/values.txt")

  // Keep columns A and B, and remove row 0221707
  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", false)
    .persist("./demo/sliced.txt")
}

class DataSciencePipelineWithFiltering(args : Args) extends Job(args) {
  // Read the data. This returns a 2D matrix (instance x feature).
  val data = read2D("exampleInput.txt")

  // Define a custom partition. If the instance is 'iid:0364354' or 'iid:0216406' then assign it to the right (test)
  // partition. In all other cases assing it to the left (train) partition.
  case class CustomPartition[S: Ordering](dim: Dimension, left: S, right: S) extends Partitioner with Assign {
    type T = S

    def assign[P <: Position](pos: P): Collection[T] = {
      if (pos.get(dim).toShortString == "iid:0364354" || pos.get(dim).toShortString == "iid:0216406") {
        Some(Left(right))
      } else {
        Some(Left(left))
      }
    }
  }

  // Perform a split of the data into a training and test set.
  val parts = data
    .partition(CustomPartition(First, "train", "test"))


  // Compute statistics on the training data. The results are written to file.
  val statistics = List(
    Count("count"),
    Moments("mean", "sd", "skewness", "kurtosis"),
    Min("min"),
    Max("max"),
    MaxAbs("max.abs"),
    Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"),
                                Histogram.entropy("entropy"),
                                Histogram.frequencyRatio("freq.ratio"))))

  val stats = parts
    .get("train")
    .reduceAndExpand(Along(First), statistics)
    .persist("./demo/stats.out")

  // Determine which features to filter based on statistics. In this case remove all features that occur for 2 or
  // fewer instances. These are removed first to prevent indicator features from being created.
  val rem1 = stats
    .which((pos: Position, con: Content) => (pos.get(Second) equ "count") && (con.value leq 2))
    .names(Over(First))

  // Also remove constant features (standard deviation is 0, or 1 category). These are removed after indicators have
  // been created.
  val rem2 = stats
    .which((pos: Position, con: Content) =>
      ((pos.get(Second) equ "sd") && (con.value equ 0)) || ((pos.get(Second) equ "num.cat") && (con.value equ 1)))
    .names(Over(First))

  // Finally remove categoricals for which an individual category has only 1 value. These are removed after binarized
  // features have been created.
  val rem3 = stats
    .which((pos: Position, con: Content) => (pos.get(Second) like ".*=.*".r) && (con.value equ 1))
    .names(Over(Second))

  // For each partition:
  //  1a/ Get the data;
  //  1b/ Remove sparse features;
  //  2/  Create indicator features;
  //  3a/ Remove constant features;
  //  3b/ Clamp features to min/max value of the training data and standardise, binarise categorical features;
  //  3c/ Remove sparse category features;
  //  4a/ Combine preprocessed data sets;
  //  4b/ Optionally fill the matrix (note: this is expensive);
  //  4c/ Save the result as pipe separated CSV for use in modelling.
  val transforms = List(
    Clamp(Second, lower="min", upper="max") andThen Standardise(Second, mean="mean", sd="sd"),
    Binarise(Second))

  for (p <- List("train", "test")) {
    val d = parts
      .get(p)
      .slice(Over(Second), rem1, false)

    val ind = d
      .transform(Indicator(Second, name="%1$s.ind"))

    val csb = d
      .slice(Over(Second), rem2, false)
      .transformWithValue(transforms, stats.toMap(Over(First)))
      .slice(Over(Second), rem3, false)

    (ind ++ csb)
      //.fill(Content(ContinuousSchema[Codex.DoubleCodex], 0))
      .writeCSV(Over(Second), "./demo/" + p + ".csv")
  }
}

class Scoring(args : Args) extends Job(args) {
  // Read the data. This returns a 2D matrix (instance x feature).
  val data = read2D("exampleInput.txt")
  // Read the statistics from the above example.
  val stats = read2D("./demo/stats.out").toMap(Over(First))
  // Read externally learned weights.
  val weights = read1D("exampleWeights.txt").toMap(Over(First))

  // For the data do:
  //  1/ Create indicators, binarise categorical, and clamp & standardise numerical features;
  //  2/ Compute the scored (as a weighted sum);
  //  3/ Save the results.
  val transforms = List(
    Indicator(Second, name="%1$s.ind"),
    Binarise(Second),
    Clamp(Second, lower="min", upper="max") andThen Standardise(Second, mean="mean", sd="sd"))

  data
    .transformWithValue(transforms, stats)
    .reduceWithValue(Over(First), WeightedSum(Second), weights)
    .persist("./demo/scores.out")
}

class DataQualityAndAnalysis(args : Args) extends Job(args) {
  // Read the data. This returns a 2D matrix (instance x feature).
  val data = read2D("exampleInput.txt")

  // For the instances:
  //  1/ Compute the number of features for each instance;
  //  2/ Save the counts;
  //  3/ Compute the moments of the counts;
  //  4/ Save the moments.
  data
    .reduce(Over(First), Count())
    .persist("./demo/feature_count.out")
    .reduceAndExpand(Along(First), Moments("mean", "sd", "skewness", "kurtosis"))
    .persist("./demo/feature_density.out")

  // For the features:
  //  1/ Compute the number of instance that have a value for each features;
  //  2/ Save the counts;
  //  3/ Compute the moments of the counts;
  //  4/ Save the moments.
  data
    .reduce(Over(Second), Count())
    .persist("./demo/instance_count.out")
    .reduceAndExpand(Along(First), Moments("mean", "sd", "skewness", "kurtosis"))
    .persist("./demo/instance_density.out")
}

class LabelWeighting(args: Args) extends Job(args) {
  // Read labels and melt the date into the instance id to generate a 1D matrix.
  val labels = read2DWithSchema("exampleLabels.txt", ContinuousSchema[Codex.DoubleCodex]())
    .melt(Second, First, ":")

  // Compute histogram over the label values.
  val histogram = labels
    .reduceAndExpand(Along(First), Histogram("%2$s", strict=true, all=true, frequency=true))

  // Compute the total number of labels and store result in a Map.
  val sum = labels
    .size(First)
    .toMap(Over(First))

  // Compute the ratio of (total number of labels) / (count for each label).
  val ratio = histogram
    .transformWithValue(Fraction(First, key=First.toString, inverse=true), sum)

  // Find the minimum ratio, and store the result as a Map.
  val min = ratio
    .reduceAndExpand(Along(First), Min("min"))
    .toMap(Over(First))

  // Divide the ratio by the minimum ratio, and store the result as a Map.
  val weights = ratio
    .transformWithValue(Fraction(First, key="min"), min)
    .toMap(Over(First))

  case class AddWeight() extends Transformer with PresentExpandedWithValue {
    type V = Map[Position1D, Content]

    // Adding the weight is a straight forward lookup by the value of the content. Also return this cell
    // (pos.append("label"), con) so no additional join is needed with the original label data.
    def present[P <: Position with ExpandablePosition](pos: P, con: Content, ext: V): CellCollection[pos.M] = {
      Some(Right(List((pos.append("label"), con), (pos.append("weight"), ext(Position1D(con.value.toShortString))))))
    }
  }

  // Re-read labels and add the computed weight.
  read2DWithSchema("exampleLabels.txt", ContinuousSchema[Codex.DoubleCodex]())
    .transformAndExpandWithValue(AddWeight(), weights)
    .persist("./demo/weighted.out")
}
