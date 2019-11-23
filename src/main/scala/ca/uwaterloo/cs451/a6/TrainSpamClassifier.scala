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
import scala.math.exp

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val shuffle = opt[Boolean](descr = "shuffle", required = false)
  verify()
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)
    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    // w is the weight vector (make sure the variable is within scope)
    val w = scala.collection.mutable.Map[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    if (args.shuffle() == true) {
      textFile = textFile
        .map(line => (scala.util.Random.nextInt(), line))
        .sortByKey()
        .map(p => p._2)
    }

    val delta = 0.002

    val trained = textFile
      .map(line => {
        // Parse input
        val elements = line.split(" ")
        val docid = elements(0)
        val isSpam = if (elements(1) == "spam") 1d else 0d
        val features = elements.drop(2).map(_.toInt)
        (0, (docid, isSpam, features))
      })
      .groupByKey(1)
      // Then run the trainer...
      .flatMap(p => {
        p._2.foreach(s => {
          val isSpam = s._2
          val features = s._3
          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        })
        w
      })

    trained.saveAsTextFile()
  }
}
