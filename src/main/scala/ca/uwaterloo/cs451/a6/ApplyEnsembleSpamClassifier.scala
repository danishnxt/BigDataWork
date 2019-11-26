package ca.uwaterloo.cs451.a6

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.Map
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf_q3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val method = opt[String](descr = "method", required = true)
  verify() //
}

object ApplyEnsembleSpamClassifier {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_q3(argv)

    val confA = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(confA)

    // working directories
    val inDirec = args.input()
    val modelPath = args.model()
    val methodType = args.method()

    // if exist delete file
    val outDirec = new Path(args.output())
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

    // populate weight Vectors
    val modelValues_X = sc.broadcast(sc.textFile(modelPath + "/part-00000").map(entry => {
      val weight = entry.substring(1,entry.length - 1).split(",")
      (weight(0).toInt, weight(1)toDouble)
    }).collectAsMap())

    val modelValues_Y = sc.broadcast(sc.textFile(modelPath + "/part-00001").map(entry => {
      val weight = entry.substring(1,entry.length - 1).split(",")
      (weight(0).toInt, weight(1)toDouble)
    }).collectAsMap())

    val modelValues_B = sc.broadcast(sc.textFile(modelPath + "/part-00002").map(entry => {
      val weight = entry.substring(1,entry.length - 1).split(",")
      (weight(0).toInt, weight(1)toDouble)
    }).collectAsMap())

    // Scores a document based on its list of features [TAKEN FROM HANDOUT]
    def spamminess(features: Array[Int], weight: scala.mutable.Map[Int, Double]) : Double = {
      var score = 0d
      features.foreach(f => if (weight.contains(f)) score += weight(f))
      score
    }

    val finalTestValues = testSamples.map(sample => {

      val doc = sample._1
      val isSpam = sample._2
      val features = sample._3 // list
      val spamValueX = spamminess(features, modelValues_X.value)
      val spamValueY = spamminess(features, modelValues_Y.value)
      val spamValueZ = spamminess(features, modelValues_B.value)

    })

    finalTestValues.saveAsTextFile(args.output()) // save to file as where needed

  }
}