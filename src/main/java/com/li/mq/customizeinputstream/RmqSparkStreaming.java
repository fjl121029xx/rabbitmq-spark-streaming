package com.li.mq.customizeinputstream;

import com.li.mq.bean.AccuracyBean;
import com.li.mq.bean.TopicRecordBean;
import com.li.mq.constants.TopicRecordConstant;
import com.li.mq.dao.ITopicRecordDao;
import com.li.mq.dao.factory.DaoFactory;
import com.li.mq.udaf.TopicRecordAccuracyUDAF;
import com.li.mq.udaf.TopicRecordCourse2AccUDAF;
import com.li.mq.udaf.TopicRecordItemNumsUDAF;
import com.li.mq.udaf.TopicRecordKnowPointUDAF;
import com.li.mq.utils.HBaseUtil;
import com.li.mq.utils.HdfsUtil;
import com.li.mq.utils.ValueUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class RmqSparkStreaming {

    private static final SimpleDateFormat sdfYMD = new SimpleDateFormat("yyyy-MM-dd");

    private static FileSystem fs;

    private static final Log logger = LogFactory.getLog(RmqSparkStreaming.class);

    static {
        try {
            fs = FileSystem.get(HdfsUtil.Configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {

        final SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("RmqSparkStreaming");
        logger.info("---------------------------");
//        try {
//            //重新编译后，删除streamingContext检查点文件
//            Path path = new Path("/rabbitmq/sparkstreaming/driver");
//            fs.delete(path, true);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        //解决驱动节点失效
//        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("hdfs://192.168.100.26:8020/rabbitmq/sparkstreaming/checkpoing", new Function0<JavaStreamingContext>() {
//            @Override
//            public JavaStreamingContext call() throws Exception {
//
//
//                JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
//                jsc.checkpoint("hdfs://192.168.100.26:8020/rabbitmq/sparkstreaming/driver");
//                return jsc;
//            }
//        });
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.checkpoint("hdfs://192.168.100.26:8020/rabbitmq/sparkstreaming/checkpoing");

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


//        JavaDStream<Row> topicResultResultVerify = correctAnalyzeVerify(topicRecord);

        JavaDStream<Row> topicResultResult = correctAnalyze(topicRecord);
//
        save2hbase(topicResultResult);
//        saveAll2hbase(topicResultResult);

//        save2mysql(topicResultResultVerify);

        topicResultResult.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }

    private static JavaDStream<Row> correctAnalyze(JavaDStream<String> topicRecord) {
        return topicRecord.transform(new Function<JavaRDD<String>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaRDD<String> rdd) throws Exception {

                JavaRDD<Row> topicRecordRow = rdd.map(new Function<String, Row>() {
                    @Override
                    public Row call(String info) throws Exception {

                        //用户id
                        Long userId = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_USERID);
                        //课件id
                        Long courseWare_id = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_COURSEWAREID);
                        //课件类型
                        Integer courseWare_type = ValueUtil.parseStr2Int(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_COURSEWARETYPE);
                        //试题Id
                        Long questionId = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_QUESTIONID);
                        //做题时长
                        Long time = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_TIME);
                        //是否正确
                        Integer correct = ValueUtil.parseStr2Int(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_CORRECT);
                        //阶段
                        Long step = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_STEP);
                        //科目
                        Long subjectId = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_SUBJECTID);
                        //所属知识点
                        String knowledgePoint = ValueUtil.parseStr2Str(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_KNOWLEDGEPOINT);
                        //视频来源
                        Integer questionSource = ValueUtil.parseStr2Int(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_QUESTIONSOURCE);
                        //提交时间
                        Long submitTime = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_SUBMITTIME);
                        String submitTimeDate = sdfYMD.format(new Date(submitTime));

                        return RowFactory.create(userId,
                                courseWare_id,
                                courseWare_type,
                                questionId,
                                time,
                                correct,
                                step,
                                subjectId,
                                knowledgePoint,
                                questionSource,
                                submitTimeDate);
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
                        DataTypes.createStructField("submitTimeDate", DataTypes.StringType, true)
                ));


                SQLContext sqlContext = new SQLContext(rdd.context());

                Dataset<Row> topicRecordDS = sqlContext.createDataFrame(topicRecordRow, schema);
                topicRecordDS.registerTempTable("tb_topic_record");

                sqlContext.udf().register("correctAnalyze", new TopicRecordAccuracyUDAF());
                sqlContext.udf().register("courseWare2topic", new TopicRecordCourse2AccUDAF());
                sqlContext.udf().register("knowledgePoint2topic", new TopicRecordKnowPointUDAF());
                sqlContext.udf().register("itemNums", new TopicRecordItemNumsUDAF());


                Dataset<Row> result = sqlContext.sql("" +
                        "select " +
                        "userId ," +
                        "correctAnalyze(correct,submitTimeDate,time) as correctAnalyze," +
                        "courseWare2topic(courseWare_id,courseWare_type,correct) as courseCorrectAnalyze, " +
                        "knowledgePoint2topic(step,subjectId,knowledgePoint,correct,time) as knowledgePointCorrectAnalyze," +
                        "count(*)," +
                        "itemNums(questionSource) as itemNums " +
                        "from tb_topic_record " +
                        "group by userId");

                result.show();
                return result.toJavaRDD();
            }
        });
    }


    private static void save2hbase(JavaDStream<Row> topicResultResult) {


        topicResultResult.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rowJavaRDD) throws Exception {
                rowJavaRDD.foreach(new VoidFunction<Row>() {
                    @Override
                    public void call(Row rowRecord) throws Exception {

                        long userId = rowRecord.getLong(0);
                        String userCorrectAnalyze = rowRecord.getString(1);
                        String courseCorrectAnalyze = rowRecord.getString(2);
                        String knowledgePointAnalyze = rowRecord.getString(3);
                        long count = rowRecord.getLong(4);
                        String itemNums = rowRecord.getString(5);

                        Long correct = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
                        Long error = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
                        Long sum = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
                        Double accuracy = ValueUtil.parseStr2Dou(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ACCURACY);
                        String submitTimeDate = ValueUtil.parseStr2Str(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUBMITTIMEDATE);
                        Long averageAnswerTime = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AVERAGEANSWERTIME);

                        averageAnswerTime = new BigDecimal(averageAnswerTime).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).longValue();


                        AccuracyBean ac = new AccuracyBean();
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
                        /**
                         * 当前用户每个知识点答题正确率
                         */
                        ac.setKnowledgePointCorrectAnalyze(knowledgePointAnalyze);
                        /**
                         * count
                         */
                        ac.setCount(count);

                        ac.setItemNums(itemNums);

                        HBaseUtil.put2hbase(AccuracyBean.TEST_HBASE_TABLE, ac);
                    }
                });
            }
        });
    }

    private static void saveAll2hbase(JavaDStream<Row> topicResultResult) {

        topicResultResult.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rowJavaRDD) throws Exception {

                rowJavaRDD.coalesce(1).foreachPartition(new VoidFunction<Iterator<Row>>() {

                    @Override
                    public void call(Iterator<Row> rowIte) throws Exception {

                        List<AccuracyBean> acs = new ArrayList<>();
                        while (rowIte.hasNext()) {

                            Row rowRecord = rowIte.next();

                            saveAll2list(acs, rowRecord);
                        }

                        HBaseUtil.putAll2hbase(AccuracyBean.TEST_HBASE_TABLE, acs);
                    }

                    private void saveAll2list(List<AccuracyBean> acs, Row rowRecord) {
                        long userId = rowRecord.getLong(0);
                        String userCorrectAnalyze = rowRecord.getString(1);
                        String courseCorrectAnalyze = rowRecord.getString(2);
                        String knowledgePointAnalyze = rowRecord.getString(3);
                        long count = rowRecord.getLong(4);


                        Long correct = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
                        Long error = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
                        Long sum = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
                        Double accuracy = ValueUtil.parseStr2Dou(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ACCURACY);
                        String submitTimeDate = ValueUtil.parseStr2Str(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUBMITTIMEDATE);
                        Long averageAnswerTime = ValueUtil.parseStr2Long(userCorrectAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AVERAGEANSWERTIME);

                        averageAnswerTime = new BigDecimal(averageAnswerTime).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).longValue();

                        AccuracyBean ac = new AccuracyBean();
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
                        /**
                         * 当前用户每个知识点答题正确率
                         */
                        ac.setKnowledgePointCorrectAnalyze(knowledgePointAnalyze);
                        /**
                         * count
                         */
                        ac.setCount(count);

                        System.out.println(ac);
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

                    rowJavaRDD.coalesce(10).foreachPartition(new VoidFunction<Iterator<Row>>() {

                        @Override
                        public void call(Iterator<Row> rowIte) throws Exception {

                            List<TopicRecordBean> trs = new ArrayList<>();
                            while (rowIte.hasNext()) {

                                Row rowRecord = rowIte.next();

                                long userId = rowRecord.getLong(0);
                                long courseWare_id = rowRecord.getLong(1);
                                int courseWare_type = rowRecord.getInt(2);
                                long questionId = rowRecord.getLong(3);
                                long time = rowRecord.getLong(4);
                                int correct = rowRecord.getInt(5);
                                String knowledgePoint = rowRecord.getString(6);
                                int questionSource = rowRecord.getInt(7);
                                String submitTimeDate = rowRecord.getString(8);

                                TopicRecordBean tr = new TopicRecordBean();
                                tr.setUserId(userId);
                                tr.setCourseWareId(courseWare_id);
                                tr.setCourseWareType(courseWare_type);
                                tr.setQuestionId(questionId);
                                tr.setTime(time);
                                tr.setCorrect(correct);
                                tr.setKnowledgePoint(knowledgePoint);
                                tr.setQuestionSource(questionSource);
                                tr.setSubmitTimeDate(submitTimeDate);

                                trs.add(tr);
                            }

                            ITopicRecordDao topicRecordDao = DaoFactory.getITopicRecordDao();
                            topicRecordDao.insertBatch(trs);

                        }
                    });
                }
            }
        });
    }
}
