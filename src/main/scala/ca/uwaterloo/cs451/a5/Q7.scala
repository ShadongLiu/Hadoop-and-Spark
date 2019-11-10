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

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val customer = sc
        .textFile(args.input() + "/customer.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
        .collectAsMap()
      val cBroadcast = sc.broadcast(customer)
      val orders = sc
        .textFile(args.input() + "/orders.tbl")
        .map(line => {
          val custKey = line.split("\\|")(1).toInt
          (line.split("\\|")(4) < date) && (cBroadcast.value.contains(custKey))
        })
        .map(line => {
          val element = line.split("\\|")
          val orderKey = element(0).toInt
          val custName = cBroadcast.value(element(1).toInt)
          val orderDate = element(4)
          val shipPriority = element(7)
          (orderKey, (custName, orderDate, shipPriority))
        })

      val lineitem = sc
        .textFile(args.input() + "/lineitem.tbl")
        .filter(line => line.split("\\|")(10) > date)
        .map(line => {
          val element = line.split("\\|")
          val extendedPrice = element(5).toDouble
          val discount = element(6).toDouble
          val revenue = extendedPrice * (1 - discount)
          (element(0).toInt, revenue)
        })
        .reduceByKey(_ + _)
        .cogroup(orders)
        .filter(p => p._2._1.size != 0 && p._2._2.size != 0)
        .map(p => {
          val cName = p._2._2.head._1
          val orderKey = p._1
          val revenue = p._2._1.head
          val orderDate = p._2._2.head._2
          val shipPriority = p._2._2.head._3
          (revenue, (cName, orderKey, revenue, orderDate, shipPriority))
        })
        .sortByKey(false)
        .collect()
        .take(10)
        .foreach(p => println(p._2._1, p._2._2, p._2._3, p._2._4, p._2._5))
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val customerDF =
        sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      val customer = customerRDD
        .map(line => (line.getInt(0), line.getString(1)))
        .collectAsMap
      val cBroadcast = sc.broadcast(customer)

      val ordersDF =
        sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val orders = ordersRDD
        .map(line => {
          val custKey = line.getInt(1)
          (line.getString(4) < date) && (cBroadcast.value.contains(custKey))
        })
        .map(line => {
          val orderKey = line.getInt(0)
          val custName = cBroadcast.value(line.getInt(1))
          val orderDate = line.getString(4)
          val shipPriority = line.getInt(0)
          (orderKey, (custName, orderDate, shipPriority))
        })

      val lineitemDF =
        sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .filter(line => line.getString(10) > date)
        .map(line => {
          val extendedPrice = line.getDouble(5)
          val discount = line.getDouble(6)
          val revenue = extendedPrice * (1 - discount)
          (line.getInt(0), revenue)
        })
        .reduceByKey(_ + _)
        .cogroup(orders)
        .filter(p => p._2._1.size != 0 && p._2._2.size != 0)
        .map(p => {
          val cName = p._2._2.head._1
          val orderKey = p._1
          val revenue = p._2._1.head
          val orderDate = p._2._2.head._2
          val shipPriority = p._2._2.head._3
          (revenue, (cName, orderKey, revenue, orderDate, shipPriority))
        })
        .sortByKey(false)
        .collect()
        .take(10)
        .foreach(p => println(p._2._1, p._2._2, p._2._3, p._2._4, p._2._5))
    }
  }
}
