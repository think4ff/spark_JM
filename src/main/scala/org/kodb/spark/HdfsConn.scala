package org.kodb.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark._

object HdfsConn {

  case class recordWeatherJeju(
                               location_code : String
                              ,location      : String
                              ,occur_date    : Int
                              ,averageTemper : Float
                              ,highTemper    : Float
                              ,lowTemper     : Float
                              ,averageCloud  : Float
                              ,rainfall      : Float
                              ,weather       : String )
  
  def main(args: Array[String]): Unit = {
    
//    val conf = new SparkConf()
//      .setAppName("WordCount")
//      .setMaster("local")
//    val sc = new SparkContext(conf)
    
    // Spark Context 생성
    val sparkConf = new SparkConf().setAppName("recordWeatherJejuData").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    // Spark SQLContext 생성
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val hdfsIn = "hdfs://192.168.10.38:8020/user/hdfs/input"
    val hdfsOut = "hdfs://192.168.10.38:8020/user/hdfs/output"

    val textFile = sc.textFile(hdfsIn + "/data_crawler_01/weatherOutput_201001_201607.csv")
//    println(textFile.first)

    import sqlContext.implicits._
    //data read
    val rddWeatherJejuListInfo = sc.textFile(hdfsIn + "/data_crawler_01/weatherOutput_201001_201607.csv")
                                   .repartition(1)

    val dfWeatherJejuListInfo = rddWeatherJejuListInfo.map(_.split(",")).
              map(p => recordWeatherJeju(
                    p(0),
                    p(1),
                    p(2).trim.toInt,
                    p(3).trim.toFloat,
                    p(4).trim.toFloat,
                    p(5).trim.toFloat,
                    p(6).trim.toFloat,
                    p(7).trim.toFloat,
                    p(8))).toDF()
                    
      dfWeatherJejuListInfo.registerTempTable("weatherTT")
//      dfWeatherJejuListInfo.show()
//      dfWeatherJejuListInfo.printSchema()
      
      dfWeatherJejuListInfo.select("weather").show()
      
      val sqlWeatherDF = sqlContext.sql("SELECT * FROM weatherTT limit 3")
      sqlWeatherDF.show()

  }

}