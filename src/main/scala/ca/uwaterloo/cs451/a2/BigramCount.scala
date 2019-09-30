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

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object BigramCount extends Tokenizer {
  
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    
    // JOB 1 //

    val unigramCount = textFile.map(line => {
      tokenize(line) // every line is now a list of tokens
    })
    .filter(line => (line.length > 1))
    .map(line => "*" :: line)
    .map(line => line.distinct).flatMap(line => {
      line
    }).map(bigram => (bigram, 1.0))
    .reduceByKey(_+_)

    val mutableMap = new scala.collection.mutable.HashMap[String, Double]

    (unigramCount.collect().toList) foreach {tup =>
      mutableMap.update(tup._1, tup._2)  
    }

    // JOB 2 //

    val bigramCount = textFile.map(line => {
      tokenize(line)
    }).filter(line => (line.length > 1))
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

    // JOB 2 AGGREGATION COMPLETE TIME TO FIND THE PMI 

    // PMI => 

    val totalVal = mutableMap.get("*").get

    val finalCount = bigramCount.map({ 
      case ((a:String, b:String), c:Double) =>
        ((a,b),c, log10((((c)/(totalVal)) / ((mutableMap.get(a).get/totalVal) * (mutableMap.get(b).get/totalVal)))))
    })
    .reduceByKey((((a,b),_,_)))
  
    finalCount.saveAsTextFile(args.output())
  }
}
