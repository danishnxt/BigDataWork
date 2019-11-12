package ca.uwaterloo.cs451.a5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.{HashMap, ListBuffer}
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf_q5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean](required = false)
  val parquet = opt[Boolean](required = false)
  verify()
}

object Q5 {

  def main(argv: Array[String]) {
    val args = new Conf_q5(argv)

    val confA = new SparkConf().setAppName("Q5 - SQL")
    val sc = new SparkContext(confA)
    val log = Logger.getLogger(getClass().getName())

    // GET ARGS

    val folder = args.input()

    val textBool = args.text()
    val parquetBool = args.parquet()

    // check what we're dealing with

    if (textBool) {
      val textFileItem = sc.textFile(folder + "/lineitem.tbl") // import from the file directly
      val textFileNation = sc.textFile(folder + "/nation.tbl") // import from the file directly
      val textFileCustomer = sc.textFile(folder + "/customer.tbl") // import from the file directly
      val textFileOrders = sc.textFile(folder + "/orders.tbl") // import from the file directly

      val lineItem_Rec = textFileItem.map(entry => (entry.split('|')(0).trim.toInt, entry.split('|')(10).substring(0, 7)))
      val nation_Rec = textFileNation.map(entry => (entry.split('|')(0).trim.toInt, entry.split('|')(1))) // reference only thru BROADCAST
      val customer_Rec = textFileCustomer.map(entry => (entry.split('|')(0).trim.toInt, entry.split('|')(3).trim.toInt))
      val orders_Rec = textFileOrders.map(entry => (entry.split('|')(0).trim.toInt, entry.split('|')(1).trim.toInt))

      val global_nation = sc.broadcast(nation_Rec.collectAsMap())
      val global_customer = sc.broadcast(customer_Rec.collectAsMap())

      val finalVal = orders_Rec.cogroup(lineItem_Rec)
        .flatMap {
          case (alpha, beta) =>
            var listD = new ListBuffer[(Int, (Int, String))]() // create a new list on the fly
            var itrA = beta._1.iterator
            var itrB = beta._2.iterator

            while (itrA.hasNext) {
              val cKey = itrA.next
              while (itrB.hasNext) {
                listD += (alpha -> (cKey, itrB.next)) // instead add value to this
              }
            }
            listD // emit this in the end
        }

      val retVal = finalVal
          .map{case (alpha, beta) => ((alpha, beta._2.toString), global_customer.value.getOrElse(beta._1, -999).asInstanceOf[Int])}.filter(entry => entry._2 != -999) // remove dead values
          .map{case (alpha, beta) => (alpha, global_nation.value.getOrElse(beta, "").toString())}.map{case (alpha, beta) => ((alpha._2, beta),1)} // restructure
          .filter{case (alpha, beta) => alpha._2 == "CANADA" || alpha._2 == "UNITED STATES"} // adding in this filter case
          .reduceByKey(_+_).map(entry => (entry._1._2, entry._1._1, entry._2)).sortBy(_._1).collect() // count, restructure and emit

      retVal.foreach(println)
    }
    else {
//
      val sparkSession = SparkSession.builder.getOrCreate

      val rddFileItem = (sparkSession.read.parquet(folder + "/lineitem")).rdd // read for a parquet file
      val rddFileNation = (sparkSession.read.parquet(folder + "/nation")).rdd // read for a parquet file
      val rddFileCustomer = (sparkSession.read.parquet(folder + "/customer")).rdd // read for a parquet file
      val rddFileOrders = (sparkSession.read.parquet(folder + "/orders")).rdd // read for a parquet file

      var lineItem_Rec = rddFileItem.map(entry => (entry(0).asInstanceOf[Int], entry(10).toString())).filter(entry => entry._4.substring(0, dateLength) == date) // date filtering
      var nation_Rec = rddFileNation.map(entry => (entry(0).asInstanceOf[Int], entry(1).toString()))
      var customer_Rec = rddFileCustomer.map(entry => (entry(0).asInstanceOf[Int], entry(3).asInstanceOf[Int]))
      var orders_Rec = rddFileOrders.map(entry => (entry(0).asInstanceOf[Int], entry(1).asInstanceOf[Int]))

      val global_nation = sc.broadcast(nation_Rec.collectAsMap())
      val global_customer = sc.broadcast(customer_Rec.collectAsMap())

      val finalVal = orders_Rec.cogroup(lineItem_Rec)
        .flatMap {
          case (alpha, beta) =>
            var listD = new ListBuffer[(Int, (Int, String))]() // create a new list on the fly
            var itrA = beta._1.iterator
            var itrB = beta._2.iterator

            while (itrA.hasNext) {
              val cKey = itrA.next
              while (itrB.hasNext) {
                listD += (alpha -> (cKey, itrB.next)) // instead add value to this
              }
            }
            listD // emit this in the end
        }

      val retVal = finalVal
        .map{case (alpha, beta) => ((alpha, beta._2.toString), global_customer.value.getOrElse(beta._1, -999).asInstanceOf[Int])}.filter(entry => entry._2 != -999) // remove dead values
        .map{case (alpha, beta) => (alpha, global_nation.value.getOrElse(beta, "").toString())}.map{case (alpha, beta) => ((alpha._2, beta),1)} // restructure
        .filter{case (alpha, beta) => alpha._2 == "CANADA" || alpha._2 == "UNITED STATES"} // adding in this filter case
        .reduceByKey(_+_).map(entry => (entry._1._2, entry._1._1, entry._2)).sortBy(_._1).collect() // count, restructure and emit

      retVal.foreach(println)
    }
  }
}