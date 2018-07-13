package com.li.mq.udaf;

import com.li.mq.bean.AccuracyBean;
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
        buffer.update(0, "knowledgePoint=0_0_'0,0,0'|correct=0|error=0|sum=0|accuracy=0|totalTime=0");
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

                if (correctRow == 0) {

                    correct++;
                } else if (correctRow == 1) {

                    error++;
                }
                sum++;

                accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
                time += timeRow;
            }

            if (!knowledgePoint.equals("0_0_'0,0,0'")) { //去掉knowledgePoint=0

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
            if (correctRow == 0) {

                correct = 1L;
                error = 0L;
            } else if (correctRow == 1) {
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


        String kn = AccuracyBean.kn(merger.getString(0), row.getString(0));

        merger.update(0, kn);

    }

    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }
}
