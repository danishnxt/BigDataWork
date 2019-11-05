package ca.uwaterloo.cs451.a5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Conf_q1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text_opt, parquet_opt)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "LineItem date - query value", required = true)
  val text_opt = opt[Boolean](required = false)
  val parquet_opt = opt[Boolean](required = false)
  verify()
}

object Q1 {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_q1(argv)

    val confA = new SparkConf().setAppName("Q1 - SQL - LineItem ship date")
    val sc = new SparkContext(confA)

    val folder = args.input()
    val date = args.date() // get the date out of the thinge

    val textBool = args.text_opt()
    val parquetBool = args.parquet_opt()

    val dateLength = date.length() // all the cases

    if (textBool == true) {
      val textFile = sc.textFile(folder + "/lineitem.tbl") // import from the file directly
      val allEntriesA = textFile.filter(entry => (entry.split('|')(10)).substring(0,dateLength) == date)
      val finalVal = allEntriesA.map(line => (1, 1)).reduceByKey(_+_).collect()

      println("HELLO CHECK HERE")
      println(finalVal(0)(1))
      finalVal.foreach(println)


    }
//    else {
//      val sparkSession = SparkSession.builder.getOrCreate
//      val lineitemDF = sparkSession.read.parquet(folder + "/lineitem")
//      val allEntriesB = lineitemDF.rdd.filter(entry => entry(10).toString().substring(0, dateLength) == date) // read for a parquet file
//      finalVal = (allEntriesB.map(line => (1,1)).reduce(_+_))(1)
//    }
  }
}