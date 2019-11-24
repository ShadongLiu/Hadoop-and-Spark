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

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val method = opt[String](descr = "method", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    //save the model as a broadcast value
    val model_x = sc.textFile(args.model() + "/part-00000")
    val w_x = model_x
      .map(m => {
        val tokens = m.substring(1, m.length() - 1).split(",")
        (tokens(0).toInt, tokens(1).toDouble)
      })
      .collectAsMap()
    val w_x_Broadcast = sc.broadcast(w_x)

    val model_y = sc.textFile(args.model() + "/part-00001")
    val w_y = model_y
      .map(m => {
        val tokens = m.substring(1, m.length() - 1).split(",")
        (tokens(0).toInt, tokens(1).toDouble)
      })
      .collectAsMap()
    val w_y_Broadcast = sc.broadcast(w_y)

    val model_b = sc.textFile(args.model() + "/part-00002")
    val w_britney = model_b
      .map(m => {
        val tokens = m.substring(1, m.length() - 1).split(",")
        (tokens(0).toInt, tokens(1).toDouble)
      })
      .collectAsMap()
    val w_britney_Broadcast = sc.broadcast(w_britney)
    // Scores a document based on its list of features.
    def spamminess(features: Array[Int], weights: scala.collection.Map[Int, Double]): Double = {
      var score = 0d
      features.foreach(
        f => if (weights.contains(f)) score += weights(f)
      )
      score
    }

    val method = args.method()
    
    val tested = textFile
      .map(line => {
        // Parse input
        val elements = line.split(" ")
        val features = elements.drop(2).map(_.toInt)
        val score_x = spamminess(features, w_x_Broadcast.value)
        val score_y = spamminess(features, w_y_Broadcast.value)
        val score_britney = spamminess(features, w_britney_Broadcast.value)
        var score = 0d
        if (method == "average") {
          score = (score_x + score_y + score_britney) / 3
        } else {
          var vote_x = if (score_x > 0) 1d else -1d
          var vote_y = if (score_y > 0) 1d else -1d
          var vote_britney = if (score_britney > 0) 1d else -1d
          score = vote_x + vote_y + vote_britney
        }
        val classify = if (score > 0) "spam" else "ham"
        (elements(0), elements(1), score, classify)
      })
    tested.saveAsTextFile(args.output())
  }
}
