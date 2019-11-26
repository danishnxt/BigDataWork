package ca.uwaterloo.cs451.a6

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Conf_q2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  verify() //
}

object ApplySpamClassifier {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_q2(argv)

    val confA = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(confA)

    // working directories
    val inDirec = args.input()
    val outDirec = new Path(args.output())
    val modelPath = args.model()

    // if exist delete file
    FileSystem.get(sc.hadoopConfiguration).delete(outDirec, true)

    // input train data
    val textSamples = sc.textFile(inDirec) // import from the file directly

    // read input data and split it
    val testSamples = textSamples.map(line => {
      val splValues = line.split(" ")
      val spamIdef = if (splValues(1) == "spam") 1 else 0
      val ftrList = splValues.drop(2)
      val ftrListEmit = ftrList.map(value => value.toInt) // convert value into an integer one
      (splValues(0), spamIdef, ftrListEmit)
    })


    // populate weight Vector
    val modelValues = sc.textFile(modelPath + "/part-00000")

    modelValues.map(entry => {
      val weight = entry.substring(1,entry.length - 1).split(",")
      (weight(0).toInt, weight(1)toDouble)
    }).collectAsMap()

    val globalWeights = sc.broadcast(modelValues) // broadcast across all system nodes

    // Scores a document based on its list of features [TAKEN FROM HANDOUT]
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    val finalTestValues = testSamples.map(sample => {

      val doc = sample._1
      val isSpam = sample._2
      val features = sample._3 // list
      val spamValue = spamminess(features)
      val valTestVal = if (spamValue > 0) 1d else 0d

      (doc, isSpam, spamValue, valTestVal)

    })

    finalTestValues.saveAsTextFile(args.output()) // save to file as where needed

  }
}