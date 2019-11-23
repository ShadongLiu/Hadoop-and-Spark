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

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[Boolean](descr = "model path", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)
    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    //save the model as a broadcast value
    val wBroadcast = sc.broadcast(sc.textFile(args.model() + "/part-00000"))

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      features.foreach(
        f => if (wBroadcast.value.contains(f)) score += wBroadcast.value(f)
      )
      score
    }

    val tested = textFile
      .map(line => {
        // Parse input
        val elements = line.split(" ")
        val features = elements.drop(2).map(_.toInt)
        val score = spamminess(features)
        val classify = if (score > 0) "spam" else "ham"

        (elements(0),elements(1),score,classify)
      })
    tested.saveAsTextFile(args.output())
  }
}
