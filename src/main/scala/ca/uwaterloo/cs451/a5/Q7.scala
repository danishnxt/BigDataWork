package ca.uwaterloo.cs451.a5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.{HashMap, ListBuffer}
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf_q7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "data input", required = true)
  val text = opt[Boolean](required = false)
  val parquet = opt[Boolean](required = false)
  verify()
}

object Q7 {

  def main(argv: Array[String]) {
    val args = new Conf_q7(argv)

    val confA = new SparkConf().setAppName("Q7 - SQL")
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
      val textFileCustomer = sc.textFile(folder + "/customer.tbl") // import from the file directly
      val textFileOrders = sc.textFile(folder + "/orders.tbl") // import from the file directly

      val lineItem_Rec = textFileItem
        .map(entry => (entry.split('|')(0).trim.toInt, entry.split('|')(5).trim.toDouble, entry.split('|')(6).trim.toDouble, entry.split('|')(10))).filter(entry => (entry._4 > date))
        .map(entry => (entry._1, entry._2*(1 - entry._3)))

      val customer_Rec = textFileCustomer
        .map(entry => {
          val spltVal = entry.split('|')
          (spltVal(0).trim.toInt, spltVal(1))
        })

      val orders_Rec = textFileOrders
        .map(entry => {
          val spltVal = entry.split('|')
          (spltVal(0).trim.toInt, spltVal(1).trim.toInt, spltVal(4), spltVal(7).trim.toInt)
        }).filter (entry => entry._3 > date).map(entry => (entry._1, (entry._2, entry._3, entry._4)))

      global_Customer = sc.broadcast(customer_Rec.collectAsMap())

      val FinalValue = orders_Rec.cogroup(lineItem_Rec)
        .flatmap {
          case(alpha, beta) =>
            var dList = new ListBuffer[(Int,(Int, String, Int, Double))]()
            var itrA = beta._1.iterator
            var itrB = beta._2.iterator

            while (itrA.hasNext) {
              val ord = itrA.next // get value out
               while (itrB.hasNext) {
                  dList += (alpha -> (ord._1, ord._2, ord._3, itrB.next))
               }
            }
            dLlist
        }

      val retVal = FinalValue.map {
//        case (alpha, beta) => ((global_Customer.value.getOrElse(beta._1, null).toString, alpha beta._2, beta._3) -> (beta._4)) #didnt' recog for some reason
        case (alpha, beta) => ((global_Customer.value.getOrElse(beta._1, null).asInstanceof[String], alpha, beta._2, beta._3) -> (beta._4))
      }.filter(x => x._1._1 != null).reduceByKey(_+_).map {
        case (alpha, beta) => (alpha._1, alpha._2, beta, alpha._3, alpha._4)
      }.collect().sortBy(_._3).reverse().take(10) // descending order

      retVal.foreach(println)

    } else {

      // get moniker is a shorter code to get the same thing done as prev questions -> useful, will not be changing previous questions however, they are functionally correct

      val sparkSession = SparkSession.builder.getOrCreate

      val rddFileItem = (sparkSession.read.parquet(folder + "/lineitem")).rdd // read for a parquet file
      val rddFileCustomer = (sparkSession.read.parquet(folder + "/customer")).rdd // read for a parquet file
      val rddFileOrders = (sparkSession.read.parquet(folder + "/orders")).rdd // read for a parquet file

      var lineItem_Rec = rddFileItem.map(entry => (entry.getInt(0), entry.getDouble(5), entry.getDouble(6), entry.getString(10))).filter(entry -> entry._4 > date).map(entry => (entry._1, entry._2*(1 - entry._3)))
      var order_Rec = rddFileOrders.map(entry => (entry.getInt(0), entry.getInt(1), entry.getString(4), entry.getInt(7))).filter(entry -> entry._3 > date).map(entry => (entry._1, (entry._2, entry._3, entry._4)))

      var customer_Rec = rddFileCustomer.map(entry => (entry.getInt(0), entry.getString(1)))
      global_Customer = sc.broadcast(customer_Rec.collectAsMap())

      val FinalValue = orders_Rec.cogroup(lineItem_Rec)
        .flatmap {
          case(alpha, beta) =>
            var dList = new ListBuffer[(Int,(Int, String, Int, Double))]()
          var itrA = beta._1.iterator
          var itrB = beta._2.iterator

          while (itrA.hasNext) {
            val ord = itrA.next // get value out
            while (itrB.hasNext) {
              dList += (alpha -> (ord._1, ord._2, ord._3, itrB.next))
            }
          }
          dLlist
        }

      val retVal = FinalValue.map {
        //        case (alpha, beta) => ((global_Customer.value.getOrElse(beta._1, null).toString, alpha beta._2, beta._3) -> (beta._4)) #didnt' recog for some reason
        case (alpha, beta) => ((global_Customer.value.getOrElse(beta._1, null).asInstanceof[String], alpha, beta._2, beta._3) -> (beta._4))
      }.filter(x => x._1._1 != null).reduceByKey(_+_).map {
        case (alpha, beta) => (alpha._1, alpha._2, beta, alpha._3, alpha._4)
      }.collect().sortBy(_._3).reverse().take(10) // descending order

      retVal.foreach(println)
    }
  }
}