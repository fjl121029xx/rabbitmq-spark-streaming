package com.li.mq.rabbit2sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object RmqSparkStreamingScala {

  def main(args: Array[String]): Unit = {

    //    val conf = new SparkConf().setMaster("local[2]").setAppName("RmqSparkStreamingScala")
    //
    //    val ssc = new StreamingContext(conf, Durations.seconds(5))
    //
    //    val streamFromRamq = ssc.receiverStream(new RabbitmqReceiver())
    //
    //
    //    val words = streamFromRamq.flatMap(s => s.split(" "))
    //
    //
    //    val ones = words.map(s => (s, 1))
    //
    //    val counts = ones.reduceByKey((v1, v2) => v1 + v2)
    //
    //    counts.print()
    //
    //    ssc.start()
    //    ssc.awaitTermination()
    //    ssc.stop()
  }

}
