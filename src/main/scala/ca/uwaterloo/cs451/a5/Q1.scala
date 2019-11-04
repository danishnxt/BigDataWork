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
  val date = opt[String](descr = "data input", required = true)
  val text_opt = opt[Boolean]()
  val parquet_opt = opt[Boolean]()
  verify()
}

object Q1 {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_q1(argv)

    val confA = new SparkConf().setAppName("Q1 - SQL")
    val sc = new SparkContext(confA)

    val folder = args.input()
    val textBool = args.text_opt()
    val date = args.date() // get the date out of the thinge
    val dateLength = date.length() // all the cases
    val parquetBool = args.parquet_opt()

    if (textBool == true) {
      textFile = sc.textFile(input + "/lineitem.tbl") // import from the file directly
      printf ("Got a text input file for now!")

    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(folder + "/lineitem")
      printf("Answer = %d" ,(sparkSession.read.parquet(input + "/lineitem")).rdd.filter(entry => entry(10).toString().substring(0,dateLength) == date)) // read for a parquet file
    }

  }
}