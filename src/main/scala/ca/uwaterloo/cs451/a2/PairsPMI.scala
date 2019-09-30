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

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner

class PairsPMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}


object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PairsPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)
    val threshold = args.threshold()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val inputFile = sc.textFile(args.input(), args.reducers())
    val totalLines = inputFile.count()

    val wordOccur = inputFile
      .flatMap(line => {
        val tokens = tokenize(line)
        //Here we don't need to slide the window since first mao just need to count the unique word
        if (tokens.length > 0) {
          val wordToSee = 40
          tokens.take(Math.min(tokens.length, wordToSee)).distinct
        }else List()
        })
      .map(word => (word, 1.0f))
      //(x,y) => x + y
      .reduceByKey(_ + _)
      //return the key-value pairs in this RDD to the master as a map
      .collectAsMap()

    val word_count_output = sc.broadcast(wordOccur)
    //second mapreduce job begins
    val pairs = inputFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val wordToSee = 40
        val words = tokens.take(Math.min(tokens.length, wordToSee)).distinct
        if (words.length > 1) {
          words.flatMap(a => words.map(b => (a, b))).filter(a => a._1 != a._2)
        }else List()
      })
      .map(pair => (pair, 1))
      .reduceByKey(_ + _)
      .sortByKey().partitionBy(new HashPartitioner(args.reducers()))
      .filter(p => p._2 >= threshold)
      //now compute the pmi value
      //ppc refers to output type pair, (pmi, count)
      .map(ppc => {
        var xVal = word_count_output.value(ppc._1._1)
        var yVal = word_count_output.value(ppc._1._2)
        var pmi = scala.math.log10(((ppc._2 * totalLines).toFloat) / (xVal * yVal))
        (ppc._1, (pmi, ppc._2.toInt))
      })
      .map(ppc => "(" +  ppc._1 + " " + ppc._2 + ")")
      pairs.saveAsTextFile(args.output())
  }
}
