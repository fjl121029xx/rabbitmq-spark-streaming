package com.li.mq.rabbit.customizeinputstream;

import com.li.mq.rabbit.bean.TopicRecordEntity;
import com.li.mq.rabbit.constants.TopicRecordConstant;
import com.li.mq.rabbit.dao.ITopicRecordDao;
import com.li.mq.rabbit.dao.factory.DaoFactory;
import com.li.mq.rabbit.utils.ValueUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import sun.java2d.pipe.SpanShapeRenderer;

import java.text.SimpleDateFormat;
import java.util.*;

public class RmqSparkStreaming {

    private static final SimpleDateFormat sdfYMD = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws InterruptedException {


        final SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("RmqSparkStreaming");
        //
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.checkpoint("D:\\tmp\\checkpoint");
        //
        JavaReceiverInputDStream<String> streamFromRamq = jsc.receiverStream(new RabbitmqReceiver());

        /**
         *
         */
        JavaPairDStream<Long, String> userid2info = streamFromRamq.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Iterator<String> strite) throws Exception {
                List<Tuple2<Long, String>> list = new ArrayList<>();

                while (strite.hasNext()) {

                    String str = strite.next();

                    String[] fields = str.split("\\|");

                    String userid_str = fields[0];

                    String userid = userid_str.split("=")[1];

                    StringBuffer info = new StringBuffer();
                    for (int i = 0; i < fields.length; i++) {
                        info.append(fields[i]).append("|");
                    }
                    info.deleteCharAt(info.length() - 1);

                    list.add(new Tuple2<>(Long.parseLong(userid), info.toString()));
                }

                return list.iterator();
            }
        });

        JavaPairDStream<Long, String> userid2infolist = userid2info.updateStateByKey(new Function2<List<String>, Optional<String>, Optional<String>>() {

            @Override
            public Optional<String> call(List<String> nowinfolist, Optional<String> original) throws Exception {

                StringBuffer sb = null;
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
                if (end == (char) '&') {
                    sb.deleteCharAt(sb.length() - 1);
                }

                return Optional.of(sb.toString());
            }
        });

        JavaDStream<String> topicRecord = userid2infolist.flatMap(new FlatMapFunction<Tuple2<Long, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<Long, String> t) throws Exception {

                return Arrays.asList(t._2.split("&")).iterator();
            }
        });
        topicRecord.print();

        JavaDStream<Row> topicResultResult = topicRecord.transform(new Function<JavaRDD<String>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaRDD<String> rdd) throws Exception {

                JavaRDD<Row> topicRecordRow = rdd.map(new Function<String, Row>() {
                    @Override
                    public Row call(String info) throws Exception {

                        //用户id
                        Long userId = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_USERID);
                        //课件id
                        Long course_ware_id = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_COURSEWAREID);
                        //试题Id
                        Long questionId = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_QUESTIONID);
                        //做题时长
                        Long time = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_TIME);
                        //是否正确
                        Integer correct = ValueUtil.parseStr2Int(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_CORRECT);
                        //所属知识点
                        Long knowledgePoint = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_KNOWLEDGEPOINT);
                        //视频来源
                        Integer questionSource = ValueUtil.parseStr2Int(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_QUESTIONSOURCE);
                        //提交时间
                        Long submitTime = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_SUBMITTIME);
                        String submitTimeDate = sdfYMD.format(new Date(submitTime));

                        return RowFactory.create(userId, course_ware_id, questionId, time, correct, knowledgePoint, questionSource, submitTimeDate);

                    }
                });

                StructType schema = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("userId", DataTypes.LongType, true),
                        DataTypes.createStructField("course_ware_id", DataTypes.LongType, true),
                        DataTypes.createStructField("questionId", DataTypes.LongType, true),
                        DataTypes.createStructField("time", DataTypes.LongType, true),
                        DataTypes.createStructField("correct", DataTypes.IntegerType, true),
                        DataTypes.createStructField("knowledgePoint", DataTypes.LongType, true),
                        DataTypes.createStructField("questionSource", DataTypes.IntegerType, true),
                        DataTypes.createStructField("submitTimeDate", DataTypes.StringType, true)
                ));


                SQLContext sqlContext = new SQLContext(rdd.context());

                Dataset<Row> topicRecordDS = sqlContext.createDataFrame(topicRecordRow, schema);
                topicRecordDS.registerTempTable("tb_topic_record");

                Dataset<Row> result = sqlContext.sql("" +
                        "select " +
                        "userId ," +
                        "course_ware_id ," +
                        "questionId ," +
                        "time,correct ," +
                        "knowledgePoint ," +
                        "questionSource ," +
                        "submitTimeDate " +
                        "from tb_topic_record");

                result.show();
                return result.toJavaRDD();
            }
        });

        topicResultResult.print();

        topicResultResult.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rowJavaRDD) throws Exception {
                rowJavaRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {

                    @Override
                    public void call(Iterator<Row> rowIte) throws Exception {

                        List<TopicRecordEntity> trs = new ArrayList<>();
                        while (rowIte.hasNext()) {

                            Row rowRecord = rowIte.next();

                            long userid = rowRecord.getLong(0);
                            long course_ware_id = rowRecord.getLong(1);
                            long questionId = rowRecord.getLong(2);
                            long time = rowRecord.getLong(3);

                            int correct = rowRecord.getInt(4);
                            long knowledgePoint = rowRecord.getLong(5);
                            int questionSource = rowRecord.getInt(6);
                            String submitTimeDate = rowRecord.getString(7);

                            TopicRecordEntity tr = new TopicRecordEntity();

                            tr.setUserId(userid);
                            tr.setCourseWareId(course_ware_id);
                            tr.setQuestionId(questionId);
                            tr.setTime(time);

                            tr.setCorrect(correct);
                            tr.setKnowledgePoint(knowledgePoint);
                            tr.setQuestionSource(questionSource);

                            tr.setSubmitTime(submitTimeDate);

                            trs.add(tr);
                        }

                        ITopicRecordDao topicRecordDao = DaoFactory.getITopicRecordDao();
                        topicRecordDao.insertBatch(trs);

                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }
}
