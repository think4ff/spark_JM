package org.kodb.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark._

object WordCount extends App {

  val conf = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local")
  val sc = new SparkContext(conf)

//  val test = sc.textFile("food.txt")
//  test.flatMap { line => line.split(" ")}
//    .map {word => (word, 1)}
//    .reduceByKey(_ + _)
//    .saveAsTextFile("food.count.txt")

  val hdfsIn = "hdfs://192.168.10.38:8020/user/hdfs/input"
  val hdfsOut = "hdfs://192.168.10.38:8020/user/hdfs/output"
  val textFile = sc.textFile(hdfsIn + "/data_crawler_01/weatherOutput_201001_201607.csv")
  
//  val textFile = sc.textFile("food.txt")
  println(textFile.first)
  
//  val counts = textFile.flatMap(line => line.split(","))
//    .map(word => (word, 1))
//    .reduceByKey(_ + _)
//  counts.saveAsTextFile(hdfsOut + "/testCount")
 
  
}