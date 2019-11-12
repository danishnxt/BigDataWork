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

      val lineItem_Rec = textFileItem.map(entry => (entry.split('|')(0), entry.split('|')(1), entry.split('|')(2), entry.split('|')(10))).filter(entry => entry._4.substring(0, dateLength) == date)
      // ordernum, part, supp, date

      val part_Rec = textFilePart.map(entry => (entry.split('|')(0), entry.split('|')(1))) // reference only
      val supplier_Rec = textFileSupplier.map(entry => (entry.split('|')(0), entry.split('|')(1))) // reference only thru BROADCAST

      val global_part = sc.broadcast(part_Rec.collectAsMap())
      val global_supplier = sc.broadcast(supplier_Rec.collectAsMap())

      val finalVal = lineItem_Rec
        .map(entry => (entry._1.toInt, global_part.value.getOrElse(entry._2, null), global_supplier.value.getOrElse(entry._3, null))) // ID, PART ID, SUPP ID
        .filter(entry => entry._2 != null) // remove non-existent values
        .filter(entry => entry._3 != null) // remove non-existent values

      val retVal = finalVal.sortBy(_._1).take(20)
      retVal.foreach(entry => (printf("(%d,%s,%s)\n", entry._1, entry._2, entry._3)))
//      result.foreach(println)

    }
    else {
//
      val sparkSession = SparkSession.builder.getOrCreate

      val rddFileItem = (sparkSession.read.parquet(folder + "/lineitem")).rdd // read for a parquet file
      val rddFilePart = (sparkSession.read.parquet(folder + "/part")).rdd // read for a parquet file
      val rddFileSupplier = (sparkSession.read.parquet(folder + "/supplier")).rdd // read for a parquet file

      var lineItem_Rec = rddFileItem.map(entry => (entry(0).toString(), entry(1).toString, entry(2).toString(), entry(10).toString())).filter(entry => entry._4.substring(0, dateLength) == date) // date filtering
      var orders_Rec = rddFilePart.map(entry => (entry(0).toString(), entry(1).toString()))
      var supplier_Rec = rddFileSupplier.map(entry => (entry(0).toString(), entry(1).toString()))

      var global_part = sc.broadcast(orders_Rec.collectAsMap())
      var global_supplier = sc.broadcast(supplier_Rec.collectAsMap())

      val finalVal = lineItem_Rec
        .map(entry => (entry._1.toInt, global_part.value.getOrElse(entry._2, null), global_supplier.value.getOrElse(entry._3, null))) // ID, PART ID, SUPP ID
        .filter(entry => entry._2 != null) // remove non-existent values
        .filter(entry => entry._3 != null) // remove non-existent values
      //
      val retVal = finalVal.sortBy(_._1).take(20)
      retVal.foreach(entry => (printf("(%d,%s,%s)\n", entry._1, entry._2, entry._3)))

    }


  }
}