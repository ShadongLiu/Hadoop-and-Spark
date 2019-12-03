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
package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper,Minutes,StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener,StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def mappingFnc(batchTime: Time, key: String, value: Option[Int], state: State[Tuple3[Int, Long, Int]]): Option[(String, Tuple3[Int, Long, Int])] = {
    var past = 0
    if (state.exists()) {
      past = state.getOption.getOrElse(0)
    }
    val cur = value.getOrElse(0)
    if ((cur >= (2 * past)) && (cur >= 10)) {
      if (key == "goldman") {
        println(
          s"Number of arrivals to Goldman Sachs has doubled from $past to $cur at $batchTime!"
        )
      } else {
        println(
          s"Number of arrivals to Citigroup has doubled from $past to $cur at $batchTime!"
        )
      }
    }

    val output = (key, (cur, batchTime.milliseconds, past))
    state.update(cur)
    Some(output)
  }
  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalsConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("TrendingArrivals")
      .getOrCreate()

    val numCompletedRDDs =
      spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    //bounding box of interest
    val g_lon_min = -74.0144185
    val g_lon_max = -74.013777
    val g_lat_min = 40.7138745
    val g_lat_max = 40.7152275

    val c_lon_min = -74.012083
    val c_lon_max = -74.009867
    val c_lat_min = 40.720053
    val c_lat_max = 40.7217236

    val wc = stream
      .map(_.split(","))
      .map(tuple => {
        val taxi_color = tuple(0)
        if (taxi_color == "yellow") {
          var yellow_lon = tuple(10).toDouble
          var yellow_lat = tuple(11).toDouble
          (yellow_lon, yellow_lat)
        } else {
          var green_lon = tuple(8).toDouble
          var green_lat = tuple(9).toDouble
          (green_lon, green_lat)
        }
      })
      .filter(
        pair => {
          val lon = pair._1
          val lat = pair._2
          ((lon > g_lon_min) && (lon < g_lon_max) && (lat > g_lat_min) && (lat < g_lat_max)) ||
          ((lon > c_lon_min) && (lon < c_lon_max) && (lat > c_lat_min) && (lat < c_lat_max))
        }
      )
      .map(pair => {
        val lon = pair._1
        val lat = pair._2
        if ((lon > g_lon_min) && (lon < g_lon_max) && (lat > g_lat_min) && (lat < g_lat_max)) {
          ("goldman", 1)
        } else {
          ("citigroup", 1)
        }
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y,
        (x: Int, y: Int) => x - y,
        Minutes(10),
        Minutes(10)
      )
      .mapWithState(StateSpec.function(mappingFnc _))
      .persist()
    
    wc.saveAsTextFiles(args.output() + "/part")

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(
        ssc,
        batchDuration.milliseconds,
        50L
      )
    }

    batchListener.waitUntilCompleted(() => ssc.stop())
  }

  class StreamingContextBatchCompletionListener(
      val ssc: StreamingContext,
      val limit: Int
  ) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(
        batchCompleted: StreamingListenerBatchCompleted
    ) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(
      sc: SparkContext,
      directoryName: String
  ): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName)
        .sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(
        s"$directoryName is not a valid directory containing part files!"
      )
    }
  }
}
