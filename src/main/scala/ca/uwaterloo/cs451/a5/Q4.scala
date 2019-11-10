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

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
    val date = args.date()

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
        .filter(line => line.split("\\|")(10).contains(date))
        .map(line => (line.split("\\|")(0).toInt, 1))
        .reduceByKey(_+_)
        .cogroup(orders)
        //(orderKey, (count, custKey))
        .filter(_._2._1.size.nonEmpty)
        .flatMap(p => {
          var list = MutableList[((Int, String), Int)]()
          val nationKey = cBroadcast.value(p._2._2.head)
          val nationName = nBroadcast.value(nationKey)
          val count = p._2._1.iterator
          println(count)
          while (count.hasNext) {
            list += (((nationKey, nationName), count.next()))
          }
          list
        })
        //key now is (nationKey, nationName)
        .reduceByKey(_+_)
        //prepare to sort by nationKey
        .map(p => (p._1._1, (p._1._2, p._2)))
        .sortByKey()
        .collect()
        .foreach(p => println(p._1, p._2._1, p._2._2))
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
        .filter(line => line.getString(10).contains(date))
        .map(line => (line.getInt(0), 1))
        .reduceByKey(_+_)
        .cogroup(orders)
        .filter(_._2._1.nonEmpty)
        .flatMap(p => {
          var list = MutableList[((Int, String), Int)]()
          val nationKey = cBroadcast.value(p._2._2.head)
          val nationName = nBroadcast.value(nationKey)
          val count = p._2._1.iterator
          while (count.hasNext) {
            list += (((nationKey, nationName), count.next()))
          }
          list
        })
        .reduceByKey(_+_)
        .map(p => (p._1._1, (p._1._2, p._2)))
        .sortByKey()
        .collect()
        .foreach(p => println(p._1, p._2._1, p._2._2))
    }
  }
}
