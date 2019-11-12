package ca.uwaterloo.cs451.a5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.{HashMap, ListBuffer}
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf_q6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "data input", required = true)
  val text = opt[Boolean](required = false)
  val parquet = opt[Boolean](required = false)
  verify()
}

object Q6 {

  def main(argv: Array[String]) {
    val args = new Conf_q6(argv)

    val confA = new SparkConf().setAppName("Q6 - SQL")
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
      val lineItem_Rec = textFileItem.map { entry =>
        val indComps = entry.split('|')
        val discountedPrice = indComps(5).toDouble * (1 - indComps(6).toDouble)
        val ActSum = discountedPrice * (1 - indComps(7).toDouble)
        ((indComps(8), indComps(9)),(indComps(4).toDouble, indComps(5).toDouble, discountedPrice, ActSum, indComps(6).toDouble, 1, indComps(10)))
      }.filter(entry => entry._2._7.substring(0, dateLength) == date))

      val AggVal = lineItem_Rec.reduceByKey((alpha, beta) => (alpha._1 + beta._1, alpha._2 + beta._2, alpha._3 + beta._3, alpha._4 + beta._4, alpha._5 + beta._5, alpha._6 + beta._6)).collect()

      AggVal.foreach(entry => {
        val total = p._2._6
        println(entry._1._1, entry._1._2, entry._2._1, entry._2._2, entry._2._3, entry._2._4, entry._2._1/total, entry._2._2/total, entry._2._5/total) // finalize total values
      })

    } else {
//
      val sparkSession = SparkSession.builder.getOrCreate
      val rddFileItem = (sparkSession.read.parquet(folder + "/lineitem")).rdd // read for a parquet file
      var lineItem_Rec = rddFileItem.map { entry =>
        val indComps = entry.split('|')
        val discountedPrice = indComps(5).toDouble * (1 - indComps(6).toDouble)
        val ActSum = discountedPrice * (1 - indComps(7).toDouble)
        ((indComps(8), indComps(9)),(indComps(4).toDouble, indComps(5).toDouble, discountedPrice, ActSum, indComps(6).toDouble, 1, indComps(10)))
      }.filter(entry => entry._2._7.substring(0, dateLength) == date))

      val AggVal = lineItem_Rec.reduceByKey((alpha, beta) => (alpha._1 + beta._1, alpha._2 + beta._2, alpha._3 + beta._3, alpha._4 + beta._4, alpha._5 + beta._5, alpha._6 + beta._6)).collect()

      AggVal.foreach(entry => {
        val total = p._2._6
        println(entry._1._1, entry._1._2, entry._2._1, entry._2._2, entry._2._3, entry._2._4, entry._2._1/total, entry._2._2/total, entry._2._5/total) // finalize total values
      })

    }
  }
}