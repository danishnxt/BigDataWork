package ca.uwaterloo.cs451.a5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Conf_q2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "data input", required = true)
  val text = opt[Boolean](required = false)
  val parquet = opt[Boolean](required = false)
  verify()
}

object Q2 {

  def main(argv: Array[String]) {
    val args = new Conf_q1(argv)

    val confA = new SparkConf().setAppName("Q2 - SQL")
    val sc = new SparkContext(confA)
    val log = Logger.getLogger(getClass().getName())

    // GET ARGS

    val folder = args.input()
    val date = args.date()

    val textBool = args.text()
    val parquetBool = args.parquet()

    val dateLength = date.length() // all the cases

    // check what we're dealing with

    if (textBool) {
      val textFileItem = sc.textFile(folder + "/lineitem.tbl") // import from the file directly
      val textFileOrder = sc.textFile(folder + "/orders.tbl") // import from the file directly

      // convert to full arrays
      lineItem_Rec = textFileItem.map(entry => entry.split('|'))
      orders_Rec = textFileOrder.map(entry => entry.split('|'))

      val correctDateOrders = textFileItem.filter(entry => (entry(10).substring(0,dateLength) == date).map(entry => (entry(0), 1)).reduceByKey(_+_).collect // giving each a purposeful value
      val prunedOrders = textFileOrder.map(entry => (entry(0), entry(6)))).reduceByKey(_+_).collect()

      mixX = correctDateOrders.cogroup(prunedOrders)
      mixX.foreach(println)

    }
      //    else {
//
//      val sparkSession = SparkSession.builder.getOrCreate
//      val textFileItem = (sparkSession.read.parquet(input + "/lineitem")).rdd // read for a parquet file
//      val textFileOrder = (sparkSession.read.parquet(input + "/orders")).rdd // read for a parquet file
//
//    }


  }
}