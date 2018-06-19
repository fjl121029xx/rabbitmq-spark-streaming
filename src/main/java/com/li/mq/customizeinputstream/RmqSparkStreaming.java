package com.li.mq.customizeinputstream;

import com.li.mq.bean.AccuracyEntity;
import com.li.mq.constants.TopicRecordConstant;
import com.li.mq.dao.IAccuracyDao;
import com.li.mq.dao.factory.DaoFactory;
import com.li.mq.udaf.TopicRecordAccuracyUDAF;
import com.li.mq.udaf.TopicRecordCourse2AccUDAF;
import com.li.mq.utils.HBaseUtil;
import com.li.mq.utils.ValueUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
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
                        DataTypes.createStructField("courseware_id", DataTypes.LongType, true),
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

                sqlContext.udf().register("correctAnalyze", new TopicRecordAccuracyUDAF());
                sqlContext.udf().register("course2topic", new TopicRecordCourse2AccUDAF());


                Dataset<Row> result = sqlContext.sql("" +
                        "select " +
                        "userId ," +
                        "correctAnalyze(correct,submitTimeDate,time) as correctAnalyze," +
                        "course2topic(courseware_id,questionId,correct) as courseCorrectAnalyze " +
                        "from tb_topic_record " +
                        "group by userId");

                result.show();
                return result.toJavaRDD();
            }
        });
//
        save2hbase(topicResultResult);

//        save2mysql(topicResultResult);

        topicResultResult.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }

    private static void save2hbase(JavaDStream<Row> topicResultResult) {
        topicResultResult.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rowJavaRDD) throws Exception {

                rowJavaRDD.coalesce(10).foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> rowIte) throws Exception {


                        List<AccuracyEntity> acs = new ArrayList<>();
                        while (rowIte.hasNext()) {

                            Row rowRecord = rowIte.next();

                            save2list(acs, rowRecord);
                        }

                        HBaseUtil.putAll2hbase(AccuracyEntity.HBASE_TABLE, acs);
                    }

                    private void save2list(List<AccuracyEntity> acs, Row rowRecord) {
                        long userId = rowRecord.getLong(0);
                        String userCorrectAnalyze = rowRecord.getString(1);
                        String courseCorrectAnalyze = rowRecord.getString(2);


                        Long correct = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
                        Long error = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
                        Long sum = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
                        Double accuracy = ValueUtil.parseStr2Dou(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ACCURACY);
                        String submitTimeDate = ValueUtil.parseStr2Str(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUBMITTIMEDATE);
                        Long averageAnswerTime = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AVERAGEANSWERTIME);

                        averageAnswerTime = new BigDecimal(averageAnswerTime).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).longValue();

                        AccuracyEntity ac = new AccuracyEntity();
                        ac.setUserId(userId);
                        ac.setSubmitTime(submitTimeDate);


                        ac.setAverageAnswerTime(averageAnswerTime);
                        ac.setCorrect(correct);
                        ac.setError(error);
                        ac.setSum(sum);
                        ac.setAccuracy(accuracy);
                        /**
                         * 当前用户每个课件答题正确率
                         */
                        ac.setCourseCorrectAnalyze(courseCorrectAnalyze);

                        acs.add(ac);
                    }
                });


            }
        });
    }

    private static void save2mysql(JavaDStream<Row> topicResultResult) {
        topicResultResult.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rowJavaRDD) throws Exception {

                if (rowJavaRDD.count() != 0) {

                    rowJavaRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {

                        @Override
                        public void call(Iterator<Row> rowIte) throws Exception {

                            List<AccuracyEntity> acs = new ArrayList<>();
                            while (rowIte.hasNext()) {

                                Row rowRecord = rowIte.next();

                                long userId = rowRecord.getLong(0);
                                String analyzeResult = rowRecord.getString(1);

                                Long correct = ValueUtil.parseStr2Long(analyzeResult, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
                                Long error = ValueUtil.parseStr2Long(analyzeResult, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
                                Long sum = ValueUtil.parseStr2Long(analyzeResult, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
                                Double accuracy = ValueUtil.parseStr2Dou(analyzeResult, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ACCURACY);
                                String submitTimeDate = ValueUtil.parseStr2Str(analyzeResult, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUBMITTIMEDATE);
                                Long evaluationAnswerTime = ValueUtil.parseStr2Long(analyzeResult, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AVERAGEANSWERTIME);

                                evaluationAnswerTime = new BigDecimal(evaluationAnswerTime).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).longValue();

                                AccuracyEntity ac = new AccuracyEntity();
                                ac.setUserId(userId);
                                ac.setSubmitTime(submitTimeDate);

                                ac.setAverageAnswerTime(evaluationAnswerTime);
                                ac.setCorrect(correct);
                                ac.setError(error);
                                ac.setSum(sum);
                                ac.setAccuracy(accuracy);

                                acs.add(ac);
                            }

                            IAccuracyDao accuracyDao = DaoFactory.getIAccuracyDao();
                            accuracyDao.insertBatch(acs);

                        }
                    });
                }
            }
        });
    }
}