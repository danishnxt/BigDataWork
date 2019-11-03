package ca.uwaterloo.cs451.a5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._

class Conf_q1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date) // , inp_type)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "data input", required = true)
//  val text = opt[Boolean](descr = "what kind of file we are to expect", required = false)
  verify()
}

object Q1 {

  val log = Logger.getLogger(getClass().getName())

  // CHECKING FULL DATE VALUE
  def dateCheckA(dataline:String, date:String): Boolean = {
    if (dataline.split('|')(10) == date)
      true
    else
      false
  }

  // CHECKING YEAR AND MONTH VALUE
  def dateCheckB(dataline:String, date:String): Boolean = {

    val value_date = dataline.split('|')(10).split('-')
    val dateSpl = date.split('-') // split up the date values

    if (value_date(0) == dateSpl(0)) // checking year
      if (value_date(1) == dateSpl(1)) // checking month
        true
      else
        false
    else
      false

  }

  // CHECKING YEAR VALUE ONLY
  def dateCheckC(dataline:String, date:String): Boolean = {

    val value_date = dataline.split('|')(10).split('-')
    val dateSpl = date.split('-') // split up the date values

    if (value_date(0) == dateSpl(0))
      true
    else
      false
  }

  def processQuery(data:org.apache.spark.rdd.RDD[String], date:String) = {
    val lines = data.map { s => s }
    if (date.lenth == 10)
      lines.filter(s => dateCheckA(s, date)) // run the filter for every s
    else if (date.lenhgth == 7)
      lines.filter(s => dateCheckB(s, date)) // run the filter for every s
    else // year only
      lines.filter(s => dateCheckC(s, date)) // run the filter for every s
  }

  def main(argv: Array[String]) {
    val args = new Conf_q1(argv)

      val type_val = argv(3)
      println(type_val)

    log.info("Input: " + args.input())
    log.info("Output: " + args.date())

    val confA = new SparkConf().setAppName("Q1 - SQL")
    val sc = new SparkContext(confA)

    val file = "/lineitem.tbl" // this is what we

    val textFile = sc.textFile(args.input() + file) // import from the file directly

    val actualLines = processQuery(textFile, args.date())
    actualLines.foreach(println)

  }
}