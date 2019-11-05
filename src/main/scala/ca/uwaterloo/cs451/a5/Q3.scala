package ca.uwaterloo.cs451.a5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Conf_q3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "data input", required = true)
  val text = opt[Boolean](required = false)
  val parquet = opt[Boolean](required = false)
  verify()
}

object Q3 {

  def main(argv: Array[String]) {
    val args = new Conf_q3(argv)

    val confA = new SparkConf().setAppName("Q3 - SQL")
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
      val textFilePart = sc.textFile(folder + "/part.tbl") // import from the file directly
      val textFileSupplier = sc.textFile(folder + "/supplier.tbl") // import from the file directly

      val lineItem_Rec = textFileItem.map(entry => (entry.split('|')(0), entry.split('|')(1), entry.split('|')(2), entry.split('|')(10))).filter(entry => entry._2.substring(0, dateLength) == date)
      // ordernum, part, supp, date

      val part_Rec = textFileOrder.map(entry => (entry.split('|')(0), entry.split('|')(1))) // reference only
      val supplier_Rec = textFileOrder.map(entry => (entry.split('|')(0), entry.split('|')(1))) // reference only thru BROADCAST

      val global_part = sc.broadcast(part_Rec)
      val global_supplier = sc.broadcast(supplier_Rec)

      val finalVal = lineItem_Rec.map(entry => (entry._1.toInt, global_part.value.filter(s => (s._1 == entry._2)).head._2, global_supplier.value.filter(s1 => (s1._2 == entry._3)).head._2))

      val finalB = final.sortBy(_._1).take(20)
      finalB.foreach(s => (printf("(%d,%s,%s)\n", s._1, s._2, s._3)))
//      result.foreach(println)

    }
//    else {
////
//      val sparkSession = SparkSession.builder.getOrCreate
//      val textFileItem = (sparkSession.read.parquet(folder + "/lineitem")).rdd // read for a parquet file
//      val textFileOrder = (sparkSession.read.parquet(folder + "/orders")).rdd // read for a parquet file
//
//      var lineItem_Rec = textFileItem.map(entry => (entry(0).toString(), entry(10).toString())).filter(entry => entry._2.substring(0, dateLength) == date)
//      var orders_Rec = textFileOrder.map(entry => (entry(0).toString(), entry(6).toString()))
//
//      val mixX = lineItem_Rec.cogroup(orders_Rec)
//      val mixXB = mixX.filter(entry => ((entry._2._1.toArray contains date) && entry._2._2.head != null))
//      val result = mixXB.map(entry => (entry._1.toInt, entry._2._2)).sortBy(_._1).take(20)
//      //
//      result.foreach(s => (printf("(%d,%s)\n", s._1, s._2.head)))
//
//    }


  }
}