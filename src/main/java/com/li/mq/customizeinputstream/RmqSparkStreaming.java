package com.li.mq.customizeinputstream;

import com.li.mq.bean.*;
import com.li.mq.constants.TopicRecordConstant;
import com.li.mq.udaf.TopicRecordAccuracyUDAF;
import com.li.mq.udaf.TopicRecordCourse2AccUDAF;
import com.li.mq.udaf.TopicRecordItemNumsUDAF;
import com.li.mq.udaf.TopicRecordKnowPointUDAF;
import com.li.mq.utils.HBaseUtil;
import com.li.mq.utils.ValueUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class RmqSparkStreaming {

    private static final SimpleDateFormat sdfYMD = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");

    public static void main(String[] args) throws InterruptedException {

        final SparkConf conf = new SparkConf()
//                .setMaster("spark://master:7077")
//                .setMaster("local[2]")
                .setAppName("RmqSparkStreaming")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        String chechkpoint = "D:\\2\\checkpoint";
        String chechkpoint = "hdfs://192.168.100.26:8020/sparkstreaming/topicrecord/checkpoint/data";

        try (JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(chechkpoint, new Function0<JavaStreamingContext>() {
            private static final long serialVersionUID = -3522596327158762004L;

            @Override
            public JavaStreamingContext call() {


                JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
                jsc.checkpoint(chechkpoint);
                jsc.remember(new Duration(24 * 3600 * 1000));

                JavaReceiverInputDStream<String> streamFromRamq = jsc.receiverStream(new RabbitmqReceiver());

                //使用updateBystate
//                JavaDStream<String> topicRecord = topicRecordUseState(streamFromRamq);
//                JavaDStream<Row> userCorrectAnalyzeResult = userCorrectAnalyze(topicRecord);
//                JavaDStream<String> repartition = streamFromRamq.repartition(18);
                JavaDStream<Row> userCorrectAnalyzeResult = userCorrectAnalyze(streamFromRamq);


                save2hbase(userCorrectAnalyzeResult);

                return jsc;
            }
        })) {


            jsc.start();
            jsc.awaitTermination();
        }


    }

    private static JavaDStream<String> topicRecordUseState(JavaReceiverInputDStream<String> streamFromRamq) {

        JavaPairDStream<Long, String> userId2info = streamFromRamq.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Long, String>() {
            private static final long serialVersionUID = -6617001840669831420L;

            @Override
            public Iterator<Tuple2<Long, String>> call(Iterator<String> strite) {
                List<Tuple2<Long, String>> list = new ArrayList<>();

                while (strite.hasNext()) {

                    String str = strite.next();

                    String[] fields = str.split("\\|");

                    String userid_str = fields[0];

                    String userid = userid_str.split("=")[1];

                    StringBuilder info = new StringBuilder();
                    for (String field : fields) {
                        info.append(field).append("|");
                    }
                    info.deleteCharAt(info.length() - 1);

                    list.add(new Tuple2<>(Long.parseLong(userid), info.toString()));
                }

                return list.iterator();
            }
        });


        JavaPairDStream<Long, String> userid2infolist = userId2info.updateStateByKey(new Function2<List<String>, Optional<String>, Optional<String>>() {

            private static final long serialVersionUID = -589467485514528883L;

            @Override
            public Optional<String> call(List<String> nowinfolist, Optional<String> original) {

                StringBuffer sb;
                if (original.isPresent()) {
                    sb = new StringBuffer(original.get());
                } else {
                    original = Optional.of("");
                    sb = new StringBuffer(original.get());
                }

                for (String info : nowinfolist) {

                    sb.append(info).append("&");
                }

                char end = sb.charAt(sb.length() - 1);
                if (end == '&') {
                    sb.deleteCharAt(sb.length() - 1);
                }

                return Optional.of(sb.toString());
            }
        });

        return userid2infolist.flatMap(new FlatMapFunction<Tuple2<Long, String>, String>() {
            private static final long serialVersionUID = 8848139165139204120L;

            @Override
            public Iterator<String> call(Tuple2<Long, String> t) {

                return Arrays.asList(t._2.split("&")).iterator();
            }
        });
    }

    private static JavaDStream<Row> userCorrectAnalyze(JavaDStream<String> topicRecord) {


        return topicRecord.transform(new Function<JavaRDD<String>, JavaRDD<Row>>() {
            private static final long serialVersionUID = -8245984755258578477L;

            @Override
            public JavaRDD<Row> call(JavaRDD<String> rdd) {

                /*JavaRDD<Row> topicRecordRow = rdd.map(new Function<String, Row>() {
                    private static final long serialVersionUID = 2779039954930815042L;

                    @Override
                    public Row call(String info) {
                        Row row = TopicRecordBean.getInto2Row(info);

                        return row;
                    }
                });*/


                JavaRDD<Row> topicRecordRow = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, Row>() {
                    private static final long serialVersionUID = 1664682336410858248L;

                    @Override
                    public Iterator<Row> call(Iterator<String> ite) throws Exception {

                        List<Row> list = new ArrayList<>();

                        while (ite.hasNext()) {

                            String info = ite.next();
                            list.add(TopicRecordBean.getInto2Row(info));
                        }
                        return list.iterator();
                    }
                });


                StructType schema = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("userId", DataTypes.LongType, true),
                        DataTypes.createStructField("courseWare_id", DataTypes.LongType, true),
                        DataTypes.createStructField("courseWare_type", DataTypes.IntegerType, true),
                        DataTypes.createStructField("questionId", DataTypes.LongType, true),
                        DataTypes.createStructField("time", DataTypes.LongType, true),
                        DataTypes.createStructField("correct", DataTypes.IntegerType, true),
                        DataTypes.createStructField("step", DataTypes.LongType, true),
                        DataTypes.createStructField("subjectId", DataTypes.LongType, true),
                        DataTypes.createStructField("knowledgePoint", DataTypes.StringType, true),
                        DataTypes.createStructField("questionSource", DataTypes.IntegerType, true),
                        DataTypes.createStructField("submitTimeDate", DataTypes.StringType, true),
                        DataTypes.createStructField("listened", DataTypes.IntegerType, true)
                ));


                SQLContext sqlContext = new SQLContext(rdd.context());

                Dataset<Row> topicRecordDS = sqlContext.createDataFrame(topicRecordRow, schema);
                topicRecordDS.registerTempTable("tb_topic_record");

                sqlContext.udf().register("correctAnalyze", new TopicRecordAccuracyUDAF());
                sqlContext.udf().register("courseWare2topic", new TopicRecordCourse2AccUDAF());
                sqlContext.udf().register("knowledgePoint2topic", new TopicRecordKnowPointUDAF());
                sqlContext.udf().register("itemNums", new TopicRecordItemNumsUDAF());
                //  courseWare2topic +  question_source
                // 确定一道题question_source      courseware_id courseware_type question_id
                Dataset<Row> result = sqlContext.sql("" +
                        "select " +
                        "userId ," +
                        "correctAnalyze(correct,submitTimeDate,time,courseWare_id,courseWare_type,questionSource,questionId) as correctAnalyze," +
                        "courseWare2topic(courseWare_id,courseWare_type,correct,questionSource,questionId) as courseCorrectAnalyze, " +
                        "knowledgePoint2topic(step,subjectId,knowledgePoint,correct,time,questionId) as knowledgePointCorrectAnalyze," +
                        "count(*)," +
                        "itemNums(questionSource,courseWare_id,courseWare_type,questionId) as itemNums " +
                        "from tb_topic_record " +
                        "group by userId");

                return result.toJavaRDD().coalesce(3);
            }
        });
    }



    private static void save2hbase(JavaDStream<Row> topicResultResult) {

        //RDD可能是空
        topicResultResult.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            private static final long serialVersionUID = -7137117405354559764L;

            @Override
            public void call(JavaRDD<Row> rdd) {

                rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    private static final long serialVersionUID = 773587774022111610L;

                    @Override
                    public void call(Iterator<Row> rowIte) throws Exception {

                        Configuration conf = HBaseConfiguration.create();
                        conf.set("hbase.zookeeper.quorum", HBaseUtil.ZK);
                        conf.set("hbase.zookeeper.property.clientPort", HBaseUtil.CL);
                        conf.set("hbase.rootdir", HBaseUtil.DIR);


                        List<AccuracyBean> acs = new ArrayList<>();

                        while (rowIte.hasNext()) {

                            Row row = rowIte.next();
                            AccuracyBean ac = AccuracyBean.row2Accuracy(row);
                            acs.add(ac);
                        }
//                        UserCourseAccuracyBean.putAllCourse2hbase(conf, acs);
//                        UserAccuracy.putAllUser2hbase(conf, acs);
//                        AccuracyBean.putAllAccuracy2hbase(conf, AccuracyBean.TEST_HBASE_TABLE, acs);
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    UserKnowledgeAccuracyBean.putAllKnow2hbase(conf, acs);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }).start();
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    UserCourseAccuracyBean.putAllCourse2hbase(conf, acs);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }).start();
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    UserAccuracy.putAllUser2hbase(conf, acs);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }).start();


//                        System.out.println(acs.size());
                    }
                });

            }
        });
    }


}