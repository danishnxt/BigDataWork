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

// Scala compiles to run within JVM - so we can use Mvn directly

// package io.bespin.scala.spark.bigram
package ca.uwaterloo.cs451.a2

// import io.bespin.scala.util.Tokenizer
import ca.uwaterloo.cs451.a2.Tokenizer // trying to import from the same folder

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.math.log10

class ConfC(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "Threshold to report pairs with", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfC(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val confC = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(confC)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers()) // read divided
    
    // JOB 1 //

    val unigramCount = textFile.map(line => {
      tokenize(line) // every line is now a list of tokens
    }) // alpha
    // .filter(line => (line.length > 1))
    .map(line => "*" :: line)
    .map(line => line.distinct).flatMap(line => {
      line
    }).map(bigram => (bigram, 1.0))
    .reduceByKey(_+_)

    val mutableMap = new scala.collection.mutable.HashMap[String, Double]

    (unigramCount.collect().toList) foreach {tup =>
      mutableMap.update(tup._1, tup._2)  
    }

    val mutableMapBC = sc.broadcast(mutableMap)
    // end of JOB 1 -> Pushing map to a broadcast var

    // JOB 2 //

    val bigramCount = textFile.map(line => {
      tokenize(line)
    }).filter(line => (line.length > 1)) // only filtering here since no pairs if life of length 1 -> still have to be counted as previous
	.map(line => line.take(40)) // taking first 40 only
    .map(line => {
      line.map(w1 => {
        line.map(w2 => {
          (w1, w2) // create pairs
          })
        }).flatten.distinct
      })
    .flatMap(line => line) // flatten everything and export
    .filter({
      case (a:String, b:String) => {
        (a != b) // must be different
      }
    }).map(bigram => (bigram, 1.0))
    .reduceByKey(_+_)

    // JOB 2 - AGGREGATION COMPLETE // TIME TO FIND THE PMI 

    // PMI => 

    // val totalVal = mutableMapBC.value.get("*").get

    // val finalCount = bigramCount.map({ 
    //   case ((a:String, b:String), c:Double) =>
    //     if (c > args.threshold) ((a,b),(log10((((c)/(totalVal)) / ((mutableMapBC.value.get(a).get/totalVal) * (mutableMapBC.value.get(b).get/totalVal)))), c))
    // }) // This might not parallelize properly hmm
  
    // finalCount.saveAsTextFile(args.output())
  }
}
