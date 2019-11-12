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
import scala.collection.mutable.MutableList

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val orders = sc
        .textFile(args.input() + "/orders.tbl")
        .map(line => {
          val element = line.split("\\|")
          //(orderKey, custKey)
          (element(0).toInt, element(1).toInt)
        })

      val customer = sc
        .textFile(args.input() + "/customer.tbl")
        .map(line => {
          val element = line.split("\\|")
          //(custKey, nationKey)
          (element(0).toInt, element(3).toInt)
        })
        .filter(p => (p._2 == 3 || p._2 == 24))
        .collectAsMap()
      val cBroadcast = sc.broadcast(customer)

      val nation = sc
        .textFile(args.input() + "/nation.tbl")
        .map(line => {
          val element = line.split("\\|")
          //(nationKey, nationName)
          (element(0).toInt, element(1))
        })
        .collectAsMap()
      val nBroadcast = sc.broadcast(nation)

      val lineitem = sc
        .textFile(args.input() + "/lineitem.tbl")
        .map(line => {
          val element = line.split("\\|")
          //(orderKey, shipdate)
          (element(0).toInt, element(10).substring(0, 7))
        })
        .cogroup(orders)
        //(orderKey, (shipdate, custKey)
        .filter(_._2._1.nonEmpty)
        .flatMap(c => {
          var list =
            MutableList[((Int, String, String), Int)]()
          if (cBroadcast.value.contains(c._2._2.head)) {
            val nationKey = cBroadcast.value(c._2._2.head)
            val nationName = nBroadcast.value(nationKey)
            val shipDates = c._2._1.iterator
            while (shipDates.hasNext) {
              list += (((nationKey, nationName, shipDates.next()), 1))
            }
          }
          list
        })
        .reduceByKey(_ + _)
        .sortBy(_._1)
        .collect()
        .foreach(c => println(c._1._1, c._1._2, c._1._3, c._2))
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val ordersDF =
        sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val orders = ordersRDD
        .map(line => (line.getInt(0), line.getInt(1)))

      val customerDF =
        sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      val customer = customerRDD
        .map(line => (line.getInt(0), line.getInt(3)))
        .filter(p => (p._2 == 3 || p._2 == 24))
        .collectAsMap
      val cBroadcast = sc.broadcast(customer)

      val nationDF =
        sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd
      val nation = nationRDD
        .map(line => (line.getInt(0), line.getString(1)))
        .collectAsMap
      val nBroadcast = sc.broadcast(nation)

      val lineitemDF =
        sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .map(line => {
          val orderKey = line.getInt(0)
          val shipDate = line.getString(10)
          (orderKey, shipDate.substring(0, shipDate.lastIndexOf('-')))
        })
        .cogroup(orders)
        .filter(_._2._1.size != 0)
        .flatMap(c => {
          var list =
            MutableList[((Int, String, String), Int)]()
          if (cBroadcast.value.contains(c._2._2.head)) {
            val nationKey = cBroadcast.value(c._2._2.head)
            val nationName = nBroadcast.value(nationKey)
            val dates = c._2._1.iterator
            while (dates.hasNext) {
              list += (((nationKey, dates.next(), nationName), 1))
            }
          }
          list
        })
        .reduceByKey(_ + _)
        .sortBy(_._1)
        .collect()
        .foreach(c => println(c._1._1, c._1._2, c._2))
    }
  }
}
