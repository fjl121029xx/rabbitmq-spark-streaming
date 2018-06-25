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

public class TopicRecordKnowPointUDAF extends UserDefinedAggregateFunction {
    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("step", DataTypes.LongType, true),
                        DataTypes.createStructField("subjectId", DataTypes.LongType, true),
                        DataTypes.createStructField("knowledgePoint", DataTypes.StringType, true),
                        DataTypes.createStructField("correct", DataTypes.IntegerType, true),
                        DataTypes.createStructField("time", DataTypes.LongType, true)
                ));
    }

    @Override
    public StructType bufferSchema() {

        return DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("knowPointAnalyze", DataTypes.StringType, true)
                )
        );
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
        //knowledgePoint=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0
        buffer.update(0, "knowledgePoint=0_0_0,0,0|correct=0|error=0|sum=0|accuracy=0|totalTime=0");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        long step = input.getLong(0);
        long subjectId = input.getLong(1);
        String knowledgePointRow = input.getString(2);
        int correctRow = input.getInt(3);
        long timeRow = input.getLong(4);

        knowledgePointRow = step + "_" + subjectId + "_" + knowledgePointRow;

        String knowledgePointAnalyzeInfo = buffer.getString(0);

        String[] knowledgePointAnalyzeInfoArr = knowledgePointAnalyzeInfo.split("\\&\\&");

        boolean notHave = false;

        StringBuilder sb = new StringBuilder();

        double accuracy = 0.00;

        for (int i = 0; i < knowledgePointAnalyzeInfoArr.length; i++) {

            String str = knowledgePointAnalyzeInfoArr[i];
            //1_2_1,2,3
            String knowledgePoint = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);

            long correct = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long error = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sum = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
            long time = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_TOTALTIME);

            if (knowledgePointRow.equals(knowledgePoint)) {

                notHave = true;

                if (correctRow == 1) {

                    correct++;
                } else if (correctRow == 0) {

                    error++;
                }
                sum++;

                accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
            }

            time += timeRow;
            if (!knowledgePoint.equals("0_0_0,0,0")) { //去掉knowledgePoint=0

                accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
                str = "knowledgePoint=" + knowledgePoint + "|" +
                        "correct=" + correct + "|" +
                        "error=" + error + "|" +
                        "sum=" + sum + "|" +
                        "accuracy=" + accuracy + "|" +
                        "totalTime=" + time;
                sb.append(str).append("&&");
            }


        }
        String str;
        if (!notHave) {
            String knowledgePoint = knowledgePointRow;
            long time = timeRow;
            long correct = 0L, error = 0L, sum = 1L;
            if (correctRow == 1) {

                correct = 1L;
                error = 0L;
            } else if (correctRow == 0) {
                correct = 0L;
                error = 1L;
            }
            accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();

            str = "knowledgePoint=" + knowledgePoint + "|" +
                    "correct=" + correct + "|" +
                    "error=" + error + "|" +
                    "sum=" + sum + "|" +
                    "accuracy=" + accuracy + "|" +
                    "totalTime=" + time;

            sb.append(str).append("&&");
        }
//        sb.replace(0,sb.indexOf("\\&\\&"),"");


        buffer.update(0, sb.toString());
    }

    @Override
    public void merge(MutableAggregationBuffer merger, Row row) {

        //大聚合值
        String knowledgePointAnalyzeInfo = merger.getString(0);
        Map<String, String> mapMerger = new HashMap<>();
        String[] cca1 = knowledgePointAnalyzeInfo.split("\\&\\&");
        for (String s : cca1) {//大聚合

            String knowledgePoint = ValueUtil.parseStr2Str(s, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            if (knowledgePoint.equals("0_0_0,0,0")) {
                continue;
            }
            mapMerger.put(knowledgePoint, s);
        }
        //本次聚合值
        String knowledgePointAnalyzeInfoOther = row.getString(0);
        Map<String, String> mapRow = new HashMap<>();
        String[] cca2 = knowledgePointAnalyzeInfoOther.split("\\&\\&");
        for (String s2 : cca2) {
            String knowledgePoint = ValueUtil.parseStr2Str(s2, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            if (knowledgePoint.equals("0_0_0,0,0")) {
                continue;
            }
            mapRow.put(knowledgePoint, s2);
        }


        Set<String> knowledgePointIdsMerge = mapMerger.keySet();
        Set<String> knowledgePointIdsRow = mapRow.keySet();

        Set<String> commonSet = new HashSet<>();
        Set<String> tmp2 = new HashSet<>();
        Set<String> mergeHaveSet = new HashSet<>();
        Set<String> tmp4 = new HashSet<>();
        Set<String> tmp5 = new HashSet<>();
        Set<String> rowHaveSet = new HashSet<>();
        Set<String> tmp7 = new HashSet<>();

        //找出都有的
        tmp2.addAll(knowledgePointIdsRow);
        commonSet.addAll(knowledgePointIdsRow);
        tmp7.addAll(knowledgePointIdsMerge);
        tmp2.removeAll(tmp7);
        commonSet.removeAll(tmp2);


        StringBuilder upda = new StringBuilder();

        for (String id : commonSet) {

            String _knowledgePointAnalyzeInfo = mapMerger.get(id);
            long knowledgePoint = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            long correct = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long error = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sum = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
            long time = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_TOTALTIME);


            String _knowledgePointAnalyzeInfoOther = mapRow.get(id);
            long correctOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long errorOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sumOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
            long timeOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_TOTALTIME);

            correct += correctOther;
            error += errorOther;
            sum += sumOther;
            time += timeOther;
            double accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();


            upda.append("knowledgePoint=" + knowledgePoint + "|" +
                    "correct=" + correct + "|" +
                    "error=" + error + "|" +
                    "sum=" + sum + "|" +
                    "accuracy=" + accuracy + "|" +
                    "totalTime=" + time).append("&&");
        }


        //找出merger有，row没有的
        mergeHaveSet.addAll(knowledgePointIdsMerge);
        tmp4.addAll(knowledgePointIdsRow);
        mergeHaveSet.removeAll(tmp4);
        for (String id : mergeHaveSet) {
            upda.append(mapMerger.get(id)).append("&&");

        }
        //找出row有，merger没有的
        tmp5.addAll(knowledgePointIdsMerge);
        rowHaveSet.addAll(knowledgePointIdsRow);
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
