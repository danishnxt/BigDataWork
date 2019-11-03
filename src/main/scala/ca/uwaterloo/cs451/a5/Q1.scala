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

  // input for data can be length -> 10
  //                              -> 7
  //                              -> 4

  // assume only year coming in for the time being
  // assume only text file for the time being

  val log = Logger.getLogger(getClass().getName())

  def dateCheck(dataline:String, date:String): Boolean = {
    val values = dataline.split("|")
    if (values(10) == data)
      true
    else
      false
  }

  def processQuery(data:Any, date:String):Any = {
    val lines = textFile.map { s => s }
    val actualLines = lines.filter(dateCheck(date))
    actualLines
  }

  def main(argv: Array[String]) {
    val args = new Conf_q1(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.date())

    val confA = new SparkConf().setAppName("Q1 - SQL")
    val sc = new SparkContext(confA)

    val file = "/lineitem.tbl" // this is what we

    val textFile = sc.textFile(args.input() + file) // import from the file directly

    // if ()
//      10
    // else if
//      7
    // else
//      4
    // hello

    actualLines = processQuery(textFile, date)
    actualLines.foreach(println)
  }
}