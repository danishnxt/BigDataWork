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

class ConfB(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val confB = new SparkConf().setAppName("ComputeBigramRelativeFrequencyPairs")
    val sc = new SparkContext(confB)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val bigramList = textFile.flatMap(line => {
      tokenize(line)
	  .filter(line => (line.length > 1))
	  .sliding(2).map(pair_list => (pair_list(0), pair_list(1))) // 
    })
	
	val bigramValue = bigramList.groupByKey(args.reducers())
	.mapValues(point => point.groupBy(identity).mapValues(_.size.toDouble/point.size))
	
	bigramValue.saveAsTextFile(args.output()) // saving to file normally
	
  }
}
