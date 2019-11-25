/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package ca.uwaterloo.cs451.a6

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.Map

class ApplyEnsembleConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model", required = true)
  val method = opt[String](descr = "method used", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  // Scores a document based on its list of features.
  def spamminess(features: Array[Int], w: Map[Int, Double]): Double = {
    var score = 0d
    features.foreach(
      f => if (w.contains(f)) score += w(f)
    )
    score
  }

  def main(argv: Array[String]) {
    val args = new ApplyEnsembleConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val method = args.method()

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.output()), true)

    //save the model as a broadcast value
    val model_x = sc.textFile(args.model() + "/part-00000")
    val w_x = model_x
      .map(m => {
        val elements = m.substring(1, m.length() - 1).split(",")
        val feature = elements(0).toInt
        val trained_weight = elements(1).toDouble
        (feature, trained_weight)
      })
      .collectAsMap()
    val x_map = sc.broadcast(w_x).value

    val model_y = sc.textFile(args.model() + "/part-00001")
    val w_y = model_y
      .map(m => {
        val elements = m.substring(1, m.length() - 1).split(",")
        val feature = elements(0).toInt
        val trained_weight = elements(1).toDouble
        (feature, trained_weight)
      })
      .collectAsMap()
    val y_map = sc.broadcast(w_y).value

    val model_b = sc.textFile(args.model() + "/part-00002")
    val w_britney = model_b
      .map(m => {
        val elements = m.substring(1, m.length() - 1).split(",")
        val feature = elements(0).toInt
        val trained_weight = elements(1).toDouble
        (feature, trained_weight)
      })
      .collectAsMap()
    val b_map = sc.broadcast(w_britney).value

    val testSet = sc.textFile(args.input())
    val tested = testSet
      .map(line => {
        // Parse input
        val elements = line.split("\\s+")
        val docid = elements(0)
        val label = elements(1)
        val features = elements.drop(2).map(_.toInt)
        val score_x = spamminess(features, x_map)
        val score_y = spamminess(features, y_map)
        val score_britney = spamminess(features, b_map)
        var ensemble_score = 0d
        if (method == "average") {
          ensemble_score = (score_x + score_y + score_britney) / 3
        } else {
          var vote = 0
          if (score_x > 0) vote += 1 else vote -= 1
          if (score_y > 0) vote += 1 else vote -= 1
          if (score_britney > 0) vote += 1 else vote -= 1
          ensemble_score = vote
        }
        val classify = if (ensemble_score > 0) "spam" else "ham"
        (docid, label, ensemble_score, classify)
      })
    tested.saveAsTextFile(args.output())
  }
}
