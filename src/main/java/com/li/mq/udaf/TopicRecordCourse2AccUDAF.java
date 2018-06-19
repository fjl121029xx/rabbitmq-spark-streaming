package com.li.mq.udaf;

import com.li.mq.constants.TopicRecordConstant;
import com.li.mq.utils.ValueUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.util.*;

public class TopicRecordCourse2AccUDAF extends UserDefinedAggregateFunction {

    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("course_ware_id", DataTypes.LongType, true),
                DataTypes.createStructField("questionId", DataTypes.LongType, true),
                DataTypes.createStructField("correct", DataTypes.IntegerType, true)
        ));
    }

    @Override
    public StructType bufferSchema() {

        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("courseCorrectAnalyze", DataTypes.StringType, true)
        ));
    }

    @Override
    public DataType dataType() {

        return DataTypes.StringType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {

        //courseWareId=0|questionIds=0|accuracy=0&&courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0
        buffer.update(0, "courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0.00");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        long courseWareIdRow = input.getLong(0);
        long questionIdsRow = input.getLong(1);
        int correctRow = input.getInt(2);

        String courseCorrectAnalyzeInfo = buffer.getString(0);

        String[] courseCorrectAnalyzeInfoArr = courseCorrectAnalyzeInfo.split("\\&\\&");


        boolean notHave = false;

        StringBuilder sb = new StringBuilder();

        long courseWareId = 0L;
        String questionIds = "";
        long correct = 0L;
        long error = 0L;
        long sum = 0L;
        double accuracy = 0.00;
        String str = "";
        for (int i = 0; i < courseCorrectAnalyzeInfoArr.length; i++) {

            str = courseCorrectAnalyzeInfoArr[i];

            courseWareId = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            questionIds = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_QUESTIONIDS);

            correct = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            error = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            sum = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
            accuracy = 0.00;

            if (courseWareIdRow == courseWareId) {

                notHave = true;

                if (correctRow == 0) {
                    correct++;
                } else if (correctRow == 1) {
                    error++;
                }
                sum++;
                accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();


                if (!questionIds.contains(Long.toString(questionIdsRow))) {
                    questionIds += "," + questionIdsRow;
                }
            }


            str = "courseWareId=" + courseWareId + "|" +
                    "questionIds=" + questionIds + "|" +
                    "correct=" + correct + "|" +
                    "error=" + error + "|" +
                    "sum=" + sum + "|" +
                    "accuracy=" + accuracy + "";

            sb.append(str).append("&&");
        }

        if (!notHave) {
            courseWareId = courseWareIdRow;
            questionIds += questionIdsRow + ",";

            if (correct == 1) {
                correct++;
            } else if (correct == 0) {
                error++;
            }
            sum++;
            accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
        }
        str = "courseWareId=" + courseWareId + "|" +
                "questionIds=" + questionIds + "|" +
                "correct=" + correct + "|" +
                "error=" + error + "|" +
                "sum=" + sum + "|" +
                "accuracy=" + accuracy + "";

        sb.append(str).append("&&");
//        sb.replace(0,sb.indexOf("\\&\\&"),"");

        buffer.update(0, sb.toString());
    }


    //courseWareId=0|questionIds=0|accuracy=0&&courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0&&
    //courseWareId=0|questionIds=0|accuracy=0&&courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0
    @Override
    public void merge(MutableAggregationBuffer merger, Row row) {


        String courseCorrectAnalyzeInfo = merger.getString(0);
        Map<Long, String> mapMerger = new HashMap<>();
        String[] cca1 = courseCorrectAnalyzeInfo.split("\\&\\&");
        for (String s : cca1) {//大聚合

            long courseWareId = ValueUtil.parseStr2Long(s, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            if (courseWareId == 0) {
                continue;
            }
            mapMerger.put(courseWareId, s);
        }

        String courseCorrectAnalyzeInfoOther = row.getString(0);
        Map<Long, String> mapRow = new HashMap<>();
        String[] cca2 = courseCorrectAnalyzeInfoOther.split("\\&\\&");
        for (String s2 : cca2) {
            long courseWareId = ValueUtil.parseStr2Long(s2, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            if (courseWareId == 0) {
                continue;
            }
            mapMerger.put(courseWareId, s2);
        }


        Set<Long> courseWareIdsMerge = mapMerger.keySet();
        Set<Long> courseWareIdsRow = mapRow.keySet();

        Set<Long> commonSet = new HashSet<>();
        Set<Long> tmp2 = new HashSet<>();
        Set<Long> mergeHaveSet = new HashSet<>();
        Set<Long> tmp4 = new HashSet<>();
        Set<Long> tmp5 = new HashSet<>();
        Set<Long> rowHaveSet = new HashSet<>();

        //找出都有的

        tmp2 = courseWareIdsRow;
        commonSet = courseWareIdsRow;
        tmp2.removeAll(courseWareIdsMerge);
        commonSet.removeAll(tmp2);


        StringBuilder upda = new StringBuilder();

        for (Long id : commonSet) {

            String _courseCorrectAnalyzeInfo = mapMerger.get(id);
            long courseWareId = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            String questionIds = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_QUESTIONIDS);
            long correct = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long error = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sum = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);


            String _courseCorrectAnalyzeInfoOther = mapRow.get(id);
            String questionIdsOther = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_QUESTIONIDS);
            long correctOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long errorOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sumOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);

            correct += correctOther;
            error += errorOther;
            sum += sumOther;

            double accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();

            List<String> qList = Arrays.asList(questionIds.split(","));
            List<String> qOtherList = Arrays.asList(questionIdsOther.split(","));
            qOtherList.removeAll(qList);
            qList.addAll(qOtherList);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < qList.size(); i++) {
                if (i != qList.size() - 1) {

                    sb.append(qList.get(i)).append(",");
                } else {
                    sb.append(qList.get(i));
                }

            }
            String newQuestionIds = sb.toString();


            upda.append("courseWareId=" + courseWareId + "|" +
                    "questionIds=" + newQuestionIds + "|" +
                    "correct=" + correct + "|" +
                    "error=" + error + "|" +
                    "sum=" + sum + "|" +
                    "accuracy=" + accuracy + "").append("&&");
        }


        //找出merger有，row没有的
        mergeHaveSet = courseWareIdsMerge;
        tmp4 = courseWareIdsRow;
        mergeHaveSet.removeAll(tmp4);
        for (Long id : mergeHaveSet) {
            upda.append(mapMerger.get(id)).append("&&");

        }
        //找出row有，merger没有的
        tmp5 = courseWareIdsMerge;
        rowHaveSet = courseWareIdsRow;
        rowHaveSet.removeAll(tmp5);
        for (Long id : rowHaveSet) {
            upda.append(mapRow.get(id)).append("&&");
        }


        merger.update(0, upda.toString());

    }

    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }
}
