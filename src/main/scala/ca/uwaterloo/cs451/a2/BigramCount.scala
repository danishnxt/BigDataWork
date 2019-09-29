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

    // TASK 1 // DO WORD COUNTS AND SAVE THEM SOMEHWERE // 

    val beta1 = textFile.map(line => {
      tokenize(line) // every line is now a list of tokens
    })

    val beta2 = beta1.filter(line => (line.length > 1)) // keep only the multiline words that have bigrams to give

    // TASK 2 // DO BIGRAM COUNTS AND SAVE THEM SOMEWHERE 

    val beta3 = beta2.map(line => "*" :: line) // count these up too pls
    val beta4 = beta3.map(line => line.distinct).flatMap(line => {
      line
    }).map(bigram => (bigram, 1))
    .reduceByKey(_+_)

    val myDict = scala.collection.mutable.Map[String, Int](beta4)

    // beta4.map((a, b) => myDict(a) = b) // update the map please let this work :)

    println("WE DID SOMETHING PLEASE LOOK HEREEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")

    beta4.foreach({
      case (a:String, b:Int) => {
        myDict(a) = b
        println("WE DID SOMETHING PLEASE LOOK HEREEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE", a, b)
      }
    })

    println(myDict)
    // println(myDict("*")) // hello there
    // beta4.saveAsTextFile(args.output())
    

    


    // val alpha1 = textFile.map(line => {
    //   tokenize(line)
    // })

    

    // val alpha2 = alpha1.map(line => {
    //   line.map(a => {
    //     line.map(b => {
    //       (a,b)
    //     })
    //   })
    // })

    // val alpha3 = alpha2.map(group => group.flatten) // try flattering on a per line scale
    // alpha3.foreach(println)

    // print ("88888888888888888888888888888888888888")
    // print ("88888888888888888888888888888888888888")
    // print ("88888888888888888888888888888888888888")
    // print ("88888888888888888888888888888888888888")

    // alpha 3 is a list that contains lists (one for each sentence) that conatains the group pairs
    // print(alpha3.length)

    // val counts = textFile
    //   .flatMap(line => {
    //     val tokens = tokenize(line)
    //     if (tokens.length > 1) tokens.map(a => ({
    //       tokens.map(b => {
    //         (a,b)
    //       })
    //     })) else List() // directly just list it up, wonder if that works!
    //   })

    //  counts.foreach(println)

    //   val countsB = counts.flatMap(identity)

    //   countsB.foreach(println)

    //   // val countsC = countsB.filter({
    //   //   case (a:String,b:String) => a != b
    //   // })
    //   CountsB.map(bigram => (bigram, 1))
    //   .reduceByKey(_ + _)
    // counts.saveAsTextFile(args.output())

    // next one should just start here

  }
}
