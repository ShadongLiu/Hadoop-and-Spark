package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
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
  

  def trackState(batchTime: Time, key: String, newValue: Option[Tuple3[Int, Long, Int]], state: State[Tuple3[Int, Long, Int]]): Option[(String, Tuple3[Int, Long, Int])] = {
      val current = newValue.getOrElse(0, 0, 0)._1
      val past = state.getOption.getOrElse(0, 0, 0)._1
      if((current >= 10) && (current >= (2*past))){
          if(key == "goldman"){
              println(s"Number of arrivals to Goldman Sachs has doubled from $past to $current at $batchTime!")
          }else{
              println(s"Number of arrivals to Citigroup has doubled from $past to $current at $batchTime!")
          }
      }

      val output = (key, (current, batchTime.milliseconds, past))
      state.update((current, batchTime.milliseconds, past))
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

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val goldman_X_min = -74.0144185
    val goldman_X_max = -74.013777
    val goldman_Y_min = 40.7138745
    val goldman_Y_max = 40.7152275

    val citigroup_X_min = -74.012083
    val citigroup_X_max = -74.009867
    val citigroup_Y_min = 40.720053
    val citigroup_Y_max = 40.7217236

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val wc = stream.map(_.split(","))

      .map(line => {
          if(line(0) == "yellow"){
              (line(10).toDouble, line(11).toDouble)
          }else{
              (line(8).toDouble, line(9).toDouble)
          }
      })
      .filter(line => ((line._1 > goldman_X_min) && (line._1 < goldman_X_max) && (line._2 > goldman_Y_min) && (line._2 < goldman_Y_max)) || ( (line._1 > citigroup_X_min) && (line._1 < citigroup_X_max) && (line._2 > citigroup_Y_min) && (line._2 < citigroup_Y_max) ))
      .map(line => {
          if((line._1 > goldman_X_min) && (line._1 < goldman_X_max) && (line._2 > goldman_Y_min) && (line._2 < goldman_Y_max)){
              ("goldman", 1)
          }else{
              ("citigroup", 1)
          }
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      //.persist()
      .map(line => (line._1, (line._2, 0L, 0)))
      .mapWithState(StateSpec.function(trackState _))


    wc.print()
    wc.saveAsTextFiles(args.output() + "/part")

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
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
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
