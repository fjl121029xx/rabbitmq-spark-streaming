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
                DataTypes.createStructField("courseWare_id", DataTypes.LongType, true),
                DataTypes.createStructField("courseWare_type", DataTypes.IntegerType, true),
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

        //courseWareId=0|correct=0|error=0|sum=0|accuracy=0
        buffer.update(0, "courseWareId=0_1|correct=0|error=0|sum=0|accuracy=0.00");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        long courseWareIdRow = input.getLong(0);
        Integer courseWareTypeRow = input.getInt(1);
        int correctRow = input.getInt(2);

        String courseCorrectAnalyzeInfo = buffer.getString(0);

        String[] courseCorrectAnalyzeInfoArr = courseCorrectAnalyzeInfo.split("\\&\\&");

        boolean notHave = false;

        StringBuilder sb = new StringBuilder();

        String courseWareId = "";
        long correct = 0L;
        long error = 0L;
        long sum = 0L;
        double accuracy = 0.00;
        String str = "";

        for (int i = 0; i < courseCorrectAnalyzeInfoArr.length; i++) {

            str = courseCorrectAnalyzeInfoArr[i];
            //courseWareId=0_1
            courseWareId = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);

            correct = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            error = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            sum = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);

            if (courseWareId.equals(courseWareIdRow + "_" + courseWareTypeRow.toString())) {

                notHave = true;

                if (correctRow == 0) {
                    correct++;
                } else if (correctRow == 1) {
                    error++;
                }
                sum++;
                accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();


            }

            if (!courseWareId.startsWith("0_")) {

                accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();

                str = "courseWareId=" + courseWareId + "|" +
                        "correct=" + correct + "|" +
                        "error=" + error + "|" +
                        "sum=" + sum + "|" +
                        "accuracy=" + accuracy + "";
                sb.append(str).append("&&");
            }


        }

        if (!notHave) {
            courseWareId = courseWareIdRow + "_" + courseWareTypeRow.toString();

            if (correctRow == 0) {

                correct = 1;
                error = 0;
            } else if (correctRow == 1) {

                correct = 0;
                error = 1;
            }
            sum = 1;
            accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
        }
        str = "courseWareId=" + courseWareId + "|" +
                "correct=" + correct + "|" +
                "error=" + error + "|" +
                "sum=" + sum + "|" +
                "accuracy=" + accuracy + "";

        sb.append(str).append("&&");
//        sb.replace(0,sb.indexOf("\\&\\&"),"");


        buffer.update(0, sb.toString());
    }


    //courseWareId=0|accuracy=0&&courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0&&
    //courseWareId=0|accuracy=0&&courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0
    @Override
    public void merge(MutableAggregationBuffer merger, Row row) {


        String courseCorrectAnalyzeInfo = merger.getString(0);
        Map<String, String> mapMerger = new HashMap<>();
        String[] cca1 = courseCorrectAnalyzeInfo.split("\\&\\&");
        for (String s : cca1) {//大聚合

            String courseWareId = ValueUtil.parseStr2Str(s, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            if (courseWareId.startsWith("0_")) {
                continue;
            }
            mapMerger.put(courseWareId, s);
        }

        String courseCorrectAnalyzeInfoOther = row.getString(0);
        Map<String, String> mapRow = new HashMap<>();
        String[] cca2 = courseCorrectAnalyzeInfoOther.split("\\&\\&");
        for (String s2 : cca2) {
            String courseWareId = ValueUtil.parseStr2Str(s2, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            if (courseWareId.startsWith("0_")) {
                continue;
            }
            mapMerger.put(courseWareId, s2);
        }


        Set<String> courseWareIdsMerge = mapMerger.keySet();
        Set<String> courseWareIdsRow = mapRow.keySet();

        Set<String> commonSet = new HashSet<>();
        Set<String> tmp2 = new HashSet<>();
        Set<String> mergeHaveSet = new HashSet<>();
        Set<String> tmp4 = new HashSet<>();
        Set<String> tmp5 = new HashSet<>();
        Set<String> rowHaveSet = new HashSet<>();

        //找出都有的

        tmp2 = courseWareIdsRow;
        commonSet = courseWareIdsRow;
        tmp2.removeAll(courseWareIdsMerge);
        commonSet.removeAll(tmp2);


        StringBuilder upda = new StringBuilder();

        for (String id : commonSet) {

            String _courseCorrectAnalyzeInfo = mapMerger.get(id);
            long courseWareId = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            long correct = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long error = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sum = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);


            String _courseCorrectAnalyzeInfoOther = mapRow.get(id);
            long correctOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long errorOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sumOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);

            correct += correctOther;
            error += errorOther;
            sum += sumOther;

            double accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();


            upda.append("courseWareId=" + courseWareId + "|" +
                    "correct=" + correct + "|" +
                    "error=" + error + "|" +
                    "sum=" + sum + "|" +
                    "accuracy=" + accuracy + "").append("&&");
        }


        //找出merger有，row没有的
        mergeHaveSet = courseWareIdsMerge;
        tmp4 = courseWareIdsRow;
        mergeHaveSet.removeAll(tmp4);
        for (String id : mergeHaveSet) {
            upda.append(mapMerger.get(id)).append("&&");

        }
        //找出row有，merger没有的
        tmp5 = courseWareIdsMerge;
        rowHaveSet = courseWareIdsRow;
        rowHaveSet.removeAll(tmp5);
        for (String id : rowHaveSet) {
            upda.append(mapRow.get(id)).append("&&");
        }


        merger.update(0, upda.toString());

    }

    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }
}
