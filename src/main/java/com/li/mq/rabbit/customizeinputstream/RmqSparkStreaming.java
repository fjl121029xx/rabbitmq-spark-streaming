package com.li.mq.rabbit.customizeinputstream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class RmqSparkStreaming {

    public static void main(String[] args) throws InterruptedException {


        final SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("RmqSparkStreaming");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));


        JavaReceiverInputDStream<String> streamFromRamq = jsc.receiverStream(new RabbitmqReceiver());


        JavaDStream<String> words = streamFromRamq.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -8701766283205910353L;

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 5519463237596341322L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 6962606899707841069L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        counts.print();


        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }
}
