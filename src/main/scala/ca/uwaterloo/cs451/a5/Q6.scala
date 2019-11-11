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

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val lineitem = sc
        .textFile(args.input() + "/lineitem.tbl")
        .filter(line => line.split("\\|")(10).contains(date))
        .map(line => {
          val element = line.split("\\|")
          val l_returnFlag = element(8)
          val l_lineStatus = element(9)
          val l_quantity = element(4).toDouble
          val l_extendedPrice = element(5).toDouble
          val l_discount = element(6).toDouble
          val l_tax = element(7).toDouble
          val discPrice = extendedPrice * (1 - l_discount)
          val charge = extendedPrice * (1 - l_discount) * (1 + l_tax)
          (
            (l_returnFlag, l_lineStatus),
            (l_quantity, l_extendedPrice, discPrice, charge, l_discount, 1)
          )
        })
        .reduceByKey(
          (x, y) =>
            (
              x._1 + y._1,
              x._2 + y._2,
              x._3 + y._3,
              x._4 + y._4,
              x._5 + y._5,
              x._6 + y._6
            )
        )
        .collect()
        .foreach(ps => {
          println(
            ps._1._1,
            ps._1._2,
            ps._2._1,
            ps._2._2,
            ps._2._3,
            ps._2._4,
            ps._2._1 / ps._2._6,
            ps._2._2 / ps._2._6,
            ps._2._5 / ps._2._6,
            ps._2._6
          )
        })
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF =
        sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .filter(line => line.getString(10).contains(date))
        .map(line => {
          val l_returnFlag = line.getString(8)
          val l_lineStatus = line.getString(9)
          val l_quantity = line.getDouble(4)
          val l_extendedPrice = line.getDouble(5)
          val l_discount = line.getDouble(6)
          val l_tax = line.getDouble(7)
          val discPrice = extendedPrice * (1 - l_discount)
          val charge = extendedPrice * (1 - l_discount) * (1 + l_tax)
          (
            (l_returnFlag, l_lineStatus),
            (l_quantity, l_extendedPrice, discPrice, charge, l_discount, 1)
          )
        })
        .reduceByKey(
          (x, y) =>
            (
              x._1 + y._1,
              x._2 + y._2,
              x._3 + y._3,
              x._4 + y._4,
              x._5 + y._5,
              x._6 + y._6
            )
        )
        .collect()
        .foreach(ps => {
          println(
            ps._1._1,
            ps._1._2,
            ps._2._1,
            ps._2._2,
            ps._2._3,
            ps._2._4,
            ps._2._1 / ps._2._6,
            ps._2._2 / ps._2._6,
            ps._2._5 / ps._2._6,
            ps._2._6
          )
        })
    }
  }
}
