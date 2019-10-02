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
import scala.collection.mutable.HashMap

class ConfA(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfA(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val confA = new SparkConf().setAppName("ComputeBigramRelativeFrequencyPairs")
    val sc = new SparkContext(confA)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    
    // JOB 1 //

    val bigramList = textFile.map(line => {
      tokenize(line) // every line is now a list of tokens
    }) // alpha
    .filter(line => (line.length > 1)) // no bigrams here -> LIST OF LINES
    .map(line => line.sliding(2).toList
    .map(pair => (pair(0), pair(1)))
    ).collect().flatten
    val reducedA = (sc.parallelize(bigramList.map(bigram => (bigram, 1.0)))).reduceByKey(_+_)

    val bigramACount = bigramList.map({
        case ((a:String,b:String)) => (a, "*")
      }) // export a first one
    .map(bigram => (bigram, 1.0))
    val reducedB = sc.parallelize(bigramACount).reduceByKey(_+_)

    val mutableMap = new scala.collection.mutable.HashMap[String, Double]

    reducedB.collect().toList foreach {
      case (((a:String, b:String),c:Double)) => {
        mutableMap.update(a,c)
      }
    }

    val reducedC = reducedA.collect() ++ reducedB.collect() // do this once
    val redC = sc.broadcast(reducedC)

    val map = sc.broadcast(mutableMap)

    val reducedFinal = redC.value.map({
      case ((a:String, b:String),c:Double) => {
        if (b == "*") ((a,b),c) else ((a,b),(c/map.value.get(a).get))
      }
    })

    sc.parallelize(reducedFinal).saveAsTextFile(args.output())

  }
}