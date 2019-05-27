package org.tremble.spark


import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by UCS-TREMBLE on 2019-04-21.
  */
object SparkStreaming_one {

  def main(args: Array[String]): Unit = {
    // StreamingContext
    val conf = new SparkConf().setAppName("spm").setMaster("local")
    val sc = new SparkContext(conf)
    val pesc = Seconds(5)
    val ssc = new StreamingContext(sc,pesc)
//    接收Dstream
    val DS = ssc.socketTextStream("192.168.30.53",4141)
    val resoult = DS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    resoult.print()
    ssc.start()
    ssc.awaitTermination()


  }

}
