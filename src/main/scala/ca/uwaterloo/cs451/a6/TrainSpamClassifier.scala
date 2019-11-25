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
import scala.collection.mutable.Map
import scala.util.Random
import scala.math.exp

class TrainConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  val shuffle = opt[Boolean](descr = "shuffle")
  verify()
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new TrainConf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Shuffle: " + args.shuffle())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)
    //delete if the output already exists
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.model()), true)

    // w is the weight vector (make sure the variable is within scope)
    val w = Map[Int, Double]()
    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    var trainingSet = sc.textFile(args.input())
    
    if (args.shuffle()) {
      trainingSet = trainingSet
        .map(line => (Random.nextInt(), line))
        .sortByKey()
        .map(_._2)
    }
    
    val trained = trainingSet
      .map(line => {
        // Parse input
        val elements = line.split("\\s+")
        val docid = elements(0)
        val label = elements(1)
        val isSpam = if (label == "spam") 1d else 0d
        val features = elements.drop(2).map(_.toInt)
        (0, (docid, isSpam, features))
      })
      .groupByKey(1)
      // Then run the trainer
      .flatMap(t => {
        t._2.foreach(s => {
          val delta = 0.002
          val isSpam = s._2
          val features = s._3
          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          // Update the weights as follows:
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

    trained.saveAsTextFile(args.model())
  }
}
