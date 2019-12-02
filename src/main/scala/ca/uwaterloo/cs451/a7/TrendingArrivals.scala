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

case class tupleClass (cur: Int, timeStamp: String, prev: Int) extends Serializable 

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def stateMap(batchTime: Time, key: String, newValue: Option[Int], state: State[tupleClass]): Option[(String, tupleClass)] = {
    var prev = 0
    if (state.exists()) {
      var curState = state.get()
      var prev = curState.cur
    }
    var cur = newValue.getOrElse(0).toInt
    var bm = batchTime.milliseconds
    if((cur >= 10) && (cur >= (2*prev))){
        if(key == "goldman"){
            println(s"Number of arrivals to Goldman Sachs has doubled from $prev to $cur at $bm!")
        }else{
            println(s"Number of arrivals to Citigroup has doubled from $prev to $cur at $bm!")
        }
    }

    val value = tupleClass(cur = cur, "%08d".format(bm), prev = prev)
    state.update(value)
    var output = (key,value)
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
    val g_lat_min = -74.0144185
    val g_lat_max = -74.013777
    val g_lon_min = 40.7138745
    val g_lon_max = 40.7152275

    val c_lat_min = -74.012083
    val c_lat_max = -74.009867
    val c_lon_min = 40.720053
    val c_lon_max = 40.7217236

    val wc = stream
      .map(_.split(","))
      .map(tuple => {
        val taxi_color = tuple(0)
        if (taxi_color == "yellow") {
          (tuple(10).toDouble, tuple(11).toDouble)
        } else {
          (tuple(8).toDouble, tuple(9).toDouble)
        }
      })
      .filter(
        pair =>
          ((pair._1 > g_lat_min) && (pair._1 < g_lat_max) && (pair._2 > g_lon_min) && (pair._2 < g_lon_max)) ||
            ((pair._1 > c_lat_min) && (pair._1 < c_lat_max) && (pair._2 > c_lon_min) && (pair._2 < c_lon_max))
      )
      .map(pair => {
        if ((pair._1 > g_lat_min) && (pair._1 < g_lat_max) && (pair._2 > g_lon_min) && (pair._2 < g_lon_max)) {
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
      .mapWithState(StateSpec.function(stateMap _))
    
    val snapShotsStreamRDD = wc.stateSnapshots()
    snapShotsStreamRDD.foreachRDD((rdd, time) =>{
      var updatedRDD = rdd.map{case(k,v) => (k,(v.cur,v.timeStamp,v.prev))}
      updatedRDD.saveAsTextFile(args.output()+ "/part-"+"%08d".format(time.milliseconds))
    })

    snapShotsStreamRDD.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc,batchDuration.milliseconds,50L)
    }

    batchListener.waitUntilCompleted(() => ssc.stop())
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext,val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName)
        .sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
