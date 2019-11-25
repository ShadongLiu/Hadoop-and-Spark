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
import scala.collection.mutable.MutableList

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

    //save the models as a broadcast value
    val model_path = List("/part-00000", "/part-00001", "part-00002")
    val models = sc.textFile(args.model() + model_path)
    val ensemble = models
      .map(line => {
        val elements = line.substring(1, line.length() - 1).split(",")
        val features = elements(0).toInt
        val trained_weights = elements(1).toDouble
        (features, trained_weights)
      })
      .collectAsMap()
    val modelBroadcast = sc.broadcast(ensemble)

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int], w: Map[Int, Double]): Double = {
      var score = 0d
      features.foreach(
        f => if (w.contains(f)) score += w(f)
      )
      score
    }

    val testSet = sc.textFile(args.input())
    val tested = testSet
      .map(line => {
        // Parse input
        val elements = line.split("\\s+")
        val docid = elements(0)
        val label = elements(1)
        val features = elements.drop(2).map(_.toInt)
        var ensembleScores = MutableList[Double]()
        for (i <- 0 until modelBroadcast.value.size) {
          ensembleScores += spamminess(features, modelBroadcast.value.get(i))
        }
        var score = 0d
        if (method == "average") {
          score = ensembleScores.sum / ensemble.size.toDouble
        } else {
          var score = 0d
          ensembleScores.foreach(s => {
            if (s > 0) score +=1d else score -=1d
          })
        }
        val classify = if (score > 0) "spam" else "ham"
        (docid, label, score, classify)
      })
    tested.saveAsTextFile(args.output())
  }
}
