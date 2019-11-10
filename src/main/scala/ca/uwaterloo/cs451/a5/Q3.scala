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
package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val part = sc
        .textFile(args.input() + "/part.tbl")
        .map(line => {
          val element = line.split("\\|")
          (element(0).toInt, element(1))
        })
        .collectAsMap()
      val pBroadcast = sc.broadcast(part)

      val supplier = sc
        .textFile(args.input() + "/supplier.tbl")
        .map(line => {
          val element = line.split("\\|")
          (element(0).toInt, element(1))
        })
        .collectAsMap()
      val sBroadcast = sc.broadcast(supplier)

      val lineitem = sc
        .textFile(args.input() + "/lineitem.tbl")
        .filter(line => line.split("\\|")(10).contains(date))
        .map(line => {
          val lines = line.split("\\|")
          val orderKey = lines(0).toInt
          val partKey = lines(1).toInt
          val suppKey = lines(2).toInt
          (orderKey, (pBroadcast.value(partKey), sBroadcast.value(suppKey)))
        })
        .sortByKey()
        .take(20)
        .foreach(p => println(p._1, p._2._1, p._2._2))
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val partDF =
        sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
      val part = partRDD
        .map(line => {
          (line.getInt(0), line.getString(1))
        })
        .collectAsMap()
      val pBroadcast = sc.broadcast(part)

      val supplierDF =
        sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd
      val supplier = supplierRDD
        .map(line => {
          (line.getInt(0), line.getString(1))
        })
        .collectAsMap()
      val sBroadcast = sc.broadcast(supplier)

      val lineitemDF =
        sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .filter(line => line.getString(10).contains(date))
        .map(line => {
          val orderKey = line.getInt(0)
          val partKey = line.getInt(1)
          val suppKey = line.getInt(2)
          (orderKey, (pBroadcast.value(partKey), sBroadcast.value(suppKey)))
        })
        .sortByKey()
        .take(20)
        .foreach(p => println(p._1, p._2._1, p._2._2))
    }
  }
}
