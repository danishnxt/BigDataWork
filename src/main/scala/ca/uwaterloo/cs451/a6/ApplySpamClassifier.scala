package ca.uwaterloo.cs451.a6

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Conf_q2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
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
      val ftrListEmit = ftrList.map(value => Int(value)) // convert value into an integer one
      (0, (splValues(0), spamIdef, ftrListEmit))
    }).groupByKey(1) // need everything passing thru same reducer

    // w is the weight vector (make sure the variable is within scope) [TAKEN FROM HANDOUT]
    val w = Map[Int, Double]()

    // populate weight Vector
    val modelValues = sc.textFile(modelPath)

    modelValues.map(entry => {
      w(Int(entry._1)) = Double(entry._2)
    })

    // weights loaded

    // Scores a document based on its list of features [TAKEN FROM HANDOUT]
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    finalTestValues = testSamples.map(sample => {

      val doc = sample._1
      val isSpam = sample._2
      val features = sample._3 // list
      val spamValue = spamminess(features)
      val valTestVal = if (spamValue > 0) 1d else 0d

      (doc, isSpam, spamValue, valTestVal)
      })
    })

    finalTestValues.saveAsTextFile(outDirec) // save to file as where needed

  }
}