package ca.uwaterloo.cs451.a5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Conf_q1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date) // , inp_type)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "data input", required = true)
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
    if (date.length == 10)
      lines.filter(s => dateCheckA(s, date)) // run the filter for every s
    else if (date.length == 7)
      lines.filter(s => dateCheckB(s, date)) // run the filter for every s
    else // year only
      lines.filter(s => dateCheckC(s, date)) // run the filter for every s
  }

  def main(argv: Array[String]) {
//    val args = new Conf_q1(argv)

    val sparkSession = SparkSession.builder.getOrCreate

    val input = argv(1)
    val date = argv(3)
    val fileType = argv(4)



//    if (fileType == "--text")
//      textFile = sc.textFile(input + "/lineitem.tbl") // import from the file directly
//    else if (fileType == "--parquet")
    val textFile = (sparkSession.read.parquet(input + "/lineitem")).rdd // read for a parquet file
    val alpha = textFile.map{s => s}
    alpha.foreach(println)


//    val confA = new SparkConf().setAppName("Q1 - SQL")
//    val sc = new SparkContext(confA)

//    val actualLines = processQuery(textFile, date)
//    actualLines.foreach(println)

  }
}