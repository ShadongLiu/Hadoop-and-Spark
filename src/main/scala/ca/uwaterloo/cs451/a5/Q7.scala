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
        .map(line => {
          val element = line.split("\\|")
          //(custKey, custName)
          (element(0).toInt, element(1))
        })
        .collectAsMap()
      val cBroadcast = sc.broadcast(customer)

      val lineitem = sc
        .textFile(args.input() + "/lineitem.tbl")
        .filter(line => line.split("\\|")(10) > date)
        .map(line => {
          val element = line.split("\\|")
          val l_orderKey = element(0).toInt
          val l_extendedPrice = element(5).toDouble
          val l_discount = element(6).toDouble
          val revenue = l_extendedPrice * (1 - l_discount)
          (l_orderKey, revenue)
        })
        .reduceByKey(_ + _)

      val orders = sc
        .textFile(args.input() + "/orders.tbl")
        .filter(line => {
          val element = line.split("\\|")
          val custKey = element(1).toInt
          val o_oderDate = element(4)
          //c_custkey = o_custkey and o_orderdate < "YYYY-MM-DD"
          (cBroadcast.value.contains(custKey)) && (o_oderDate < date)
        })
        .map(line => {
          val element = line.split("\\|")
          val o_orderKey = element(0).toInt
          val custKey = element(1).toInt
          val custName = cBroadcast.value(custKey)
          val o_orderDate = element(4)
          val o_shipPriority = element(7)
          (o_orderKey, (custName, o_orderDate, o_shipPriority))
        })
        //reduce-side join, using cogroup
        .cogroup(lineitem)
        .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
        .map(p => {
          val cName = p._2._1.head._1
          val orderKey = p._1
          val revenue = p._2._2.head
          val o_orderDate = p._2._1.head._2
          val o_shipPriority = p._2._1.head._3
          (revenue, (cName, orderKey, revenue, o_orderDate, o_shipPriority))
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

      val lineitemDF =
        sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .filter(line => line.getString(10) > date)
        .map(line => {
          val l_orderKey = line.getInt(0)
          val l_extendedPrice = line.getDouble(5)
          val l_discount = line.getDouble(6)
          val revenue = l_extendedPrice * (1 - l_discount)
          (l_orderKey, revenue)
        })
        .reduceByKey(_ + _)

      val ordersDF =
        sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val orders = ordersRDD
        .filter(line => {
          val custKey = line.getInt(1)
          val o_orderDate = line.getString(4)
          (cBroadcast.value.contains(custKey)) && (o_orderDate < date)
        })
        .map(line => {
          val o_orderKey = line.getInt(0)
          val custKey = line.getInt(1)
          val custName = cBroadcast.value(custKey)
          val o_orderDate = line.getString(4)
          val o_shipPriority = line.getInt(7)
          (o_orderKey, (custName, o_orderDate, o_shipPriority))
        })
        .cogroup(lineitem)
        .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
        .map(p => {
          val cName = p._2._1.head._1
          val orderKey = p._1
          val revenue = p._2._2.head
          val o_orderDate = p._2._1.head._2
          val o_shipPriority = p._2._1.head._3
          (revenue, (cName, orderKey, revenue, o_orderDate, o_shipPriority))
        })
        .sortByKey(false)
        .collect()
        .take(10)
        .foreach(p => println(p._2._1, p._2._2, p._2._3, p._2._4, p._2._5))
    }
  }
}
