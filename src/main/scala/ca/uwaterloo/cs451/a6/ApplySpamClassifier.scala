package ca.uwaterloo.cs451.a6

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Conf_q1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  verify() //
}

object ApplySpamClassifier {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_q1(argv)

    val confA = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(confA)

    // working directories
    val outDirec = new Path(args.model()) // output model file here
    val inputFolder = args.input()

    // if exist delete file
    FileSystem.get(sc.hadoopConfiguration).delete(outDirec, true)

    // input train data
    val textSamples = sc.textFile(folder + "/lineitem.tbl") // import from the file directly

    if (args.shuffle()) { // shuffle lines in the text file in place
      r = scala.util.Random
      textSamples = textSamples.map(line => {
        (r.nextInt, line)
      }).sortByKey().map(value => value._2)
    }

    // read input data and split it
    val trainSamples = textSamples.map(line => {
      val splValues = line.split(" ")
      val spamIdef = if (splValues(1) == "spam") 1 else 0
      val ftrList = splValues.drop(2)
      val ftrListEmit = ftrList.map(value => Int(value)) // convert value into an integer one
      (0, (splValues(0), spamIdef, ftrListEmit))
    }).groupByKey(1) // need everything passing thru same reducer

    // w is the weight vector (make sure the variable is within scope) [TAKEN FROM HANDOUT]
    val w = Map[Int, Double]()

    // Scores a document based on its list of features [TAKEN FROM HANDOUT]
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    // This is the main learner: [TAKEN FROM HANDOUT]
    val delta = 0.002

    trainSamples.map(sample => {

      val isSpam = sample._2
      val features = sample._3 // list

      // Update the weights as follows: [TAKEN FROM HANDOUT]
      val score = spamminess(features)
      val prob = 1.0 / (1 + exp(-score))

      features.foreach(f => {
        if (w.contains(f)) {
          w(f) += (isSpam - prob) * delta
        } else {
          w(f) = (isSpam - prob) * delta
        }

      })
      w // emit the weights
    })

    w.saveAsTextFile(outDirec) // save to file as where needed

  }
}