/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import scala.collection.mutable.ListBuffer
import org.rogach.scallop._
import scala.collection.mutable._
import org.apache.spark.streaming._

import scala.collection.mutable

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  case class myTup(current: Int, timeS: String, pVal:Int) extends Serializable

  def udpateFunc(bTimes: Time, key: String, value: Option[Int], state: State[myTup]): Option[(String, myTup)] = {

    var p = 0
    if (state.exists()) {
      p = state.get().current
    }

    var c = value.getOrElse(0).toInt
    var bTime = bTimes.milliseconds
    if ((c >= 10) && (c >= 2*p)) {
      if (key == ("goldman"))
      println(s"Number of arrivals to Goldman Sachs has doubles from $p to $c at $bTime!")
      else
      println(s"Number of arrivals to Citigroup has doubles from $p to $c at $bTime!")
    }
    var t = myTup(current = c, timeS = "%08d".format(bTime), pVal = p)
    //      state.udpete*
    //      state.update(c)
    state.update(t)
    Some((key, t))
  }

  def main(argv: Array[String]): Unit = {
    val args = new RegionEventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("TrendingArrivals")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    // GOLDMAN CO-ORDINATES
    val BB_1_CORD_G_A = (-74.0141012, 40.7152191)
    val BB_1_CORD_G_B = (-74.013777, 40.7152275)
    val BB_1_CORD_G_C = (-74.0141027, 40.7138745)
    val BB_1_CORD_G_D = (-74.0144185, 40.7140753)

    // range x val => D // B
    // range y val => C // B

    val G_CD_X_MIN = BB_1_CORD_G_D._1
    val G_CD_X_MAX = BB_1_CORD_G_B._1

    val G_CD_Y_MIN = BB_1_CORD_G_C._2
    val G_CD_Y_MAX = BB_1_CORD_G_B._2

    // CITI CO-ORDINATES
    val BB_2_CORD_C_A = (-74.011869, 40.7217236)
    val BB_2_CORD_C_B = (-74.009867, 40.721493)
    val BB_2_CORD_C_C = (-74.010140, 40.720053)
    val BB_2_CORD_C_D = (-74.012083, 40.720267)

    // range x val => D // B
    // range y val => C // A

    val C_CD_X_MIN = BB_2_CORD_C_D._1
    val C_CD_X_MAX = BB_2_CORD_C_B._1

    val C_CD_Y_MIN = BB_2_CORD_C_C._2
    val C_CD_Y_MAX = BB_2_CORD_C_A._2

      // few things to do
      // 1 check for green or not green?

    val output = StateSpec.function(udpateFunc _)

    val wc = stream.map(_.split(","))
      .flatMap(trip => {
        var long = 0D
        val lat = 0D
        if (trip(0) == "yellow")
        {
          long = trip(10).toDouble
          lat = trip(11).toDouble
        } else {
          long = trip(8).toDouble
          lat = trip(9).toDouble
        } // filtering complete

        if ((long > G_CD_X_MIN) && (long < G_CD_X_MAX) && (lat > G_CD_Y_MIN) && (lat < G_CD_Y_MAX)) {
          var myList = new ListBuffer[Tuple2[String, Int]]()
          var retVal:Tuple2[String, Int] = ("goldman", 1)
          myList += retVal
          retVal
        } else if ((long > C_CD_X_MIN) && (long < C_CD_X_MAX) && (lat > C_CD_Y_MIN) && (lat < C_CD_Y_MAX)) {
          var myList = new ListBuffer[Tuple2[String, Int]]()
          var retVal:Tuple2[String, Int] = ("citigroup", 1)
          myList += retVal
          retVal
        } else {
          List()
        }
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
