package ca.uwaterloo.cs451.a5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._

class Conf_q1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, inp_type)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "data input", required = true)
  val inp_type = opt[String](descr = "what kind of file we are to expect", required = true)
  verify()
}

object Q1 {

  // input for data can be length -> 10
  //                              -> 7
  //                              -> 4

  // assume only year coming in for the time being
  // assume only text file for the time being

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_q1(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.date())
    log.info("Number of reducers: " + args.inp_type())

    val confA = new SparkConf().setAppName("Q1 - SQL")
    val sc = new SparkContext(confA)

    val file = "/lineitem.tbl" // this is what we

    val textFile = sc.textFile(args.input() + file) // import from the file directly
    // shipdate -> 10th value

    val lines = split{ s => s}

    lines.foreach(println)
    System.out.println("This is a test of what we're going to be doing today!")

  }
}