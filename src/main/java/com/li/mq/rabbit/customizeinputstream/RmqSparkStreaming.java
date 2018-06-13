package com.li.mq.rabbit.customizeinputstream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class RmqSparkStreaming {

    public static void main(String[] args) throws InterruptedException {


        final SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("RmqSparkStreaming");
        //
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        //
        JavaReceiverInputDStream<String> streamFromRamq = jsc.receiverStream(new RabbitmqReceiver());


        JavaDStream<String> words = streamFromRamq.flatMap(s -> Arrays.asList(s.split("|")).iterator());

        JavaPairDStream<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairDStream<String, Integer> counts = ones.reduceByKey((v1, v2) -> v1 + v2);

        counts.print();


        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }
}
