package com.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

object SparkSelectiveWordcount {

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())

    if (arg.length < 3) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: SparkSelectiveWordcount <file-with-words> <path-to-files> <output-path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("SparkSelectiveWordcount")
    val sc = new SparkContext(conf)

    val setOfNames = arg(0)
    val pathToFiles = arg(1)
    val outputPath = arg(2)

    logger.info("=> setOfNames \"" + setOfNames + "\"")
    logger.info("=> pathToFiles \"" + pathToFiles + "\"")
    logger.info("=> outputPath \"" + outputPath + "\"")

    val names = sc.textFile(setOfNames)
    val files = sc.textFile(pathToFiles)

    val namesArray = names.collect()
    
    val selectiveWordCount = files
    	.flatMap(_.split(","))
    	.filter(namesArray.contains(_))
    	.map((_,1))
    	.reduceByKey((_+_))

    selectiveWordCount.saveAsTextFile(outputPath)

  }
}
