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
import java.util.Arrays;

public class TopicRecordAccuracyUDAF extends UserDefinedAggregateFunction {

    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("correct", DataTypes.IntegerType, true),
                        DataTypes.createStructField("submitTimeDate", DataTypes.StringType, true),
                        DataTypes.createStructField("time", DataTypes.LongType, true)
                )
        );
    }

    @Override
    public StructType bufferSchema() {

        return DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("accuracybuffer", DataTypes.StringType, true)
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

        buffer.update(0, "correct=0|error=0|sum=0|accuracy=0.00|submitTimeDate=0000-00-00|evaluationAnswerTime=000000");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row inputRow) {

        int correct = inputRow.getInt(0);
        String submitTimeDate = inputRow.getString(1);
        long time = inputRow.getLong(2);

        String last = buffer.get(0).toString();

        Integer correctLast = ValueUtil.parseStr2Int(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
        Integer errorLast = ValueUtil.parseStr2Int(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
        Integer sumtLast = ValueUtil.parseStr2Int(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
        Long evaluationAnswerTime = ValueUtil.parseStr2Long(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_EVALUATIONANSWERTIME);


        sumtLast++;

        if (correct == 0) {
            correctLast++;
        } else if (correct == 1) {
            errorLast++;
        }

        evaluationAnswerTime += time;
        double accuracy = new BigDecimal(correctLast).divide(new BigDecimal(sumtLast), 2, BigDecimal.ROUND_HALF_UP).doubleValue();


        buffer.update(0, "correct=" + correctLast +
                "|error=" + errorLast +
                "|sum=" + sumtLast +
                "|accuracy=" + accuracy +
                "|submitTimeDate=" + submitTimeDate +
                "|evaluationAnswerTime=" + evaluationAnswerTime);
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

        String accuracyAnalyze = buffer1.getString(0);
        Integer correctMerge = ValueUtil.parseStr2Int(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
        Integer errorMerge = ValueUtil.parseStr2Int(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
        Integer sumMerge = ValueUtil.parseStr2Int(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
        Long evaluationAnswerTimeMerge = ValueUtil.parseStr2Long(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_EVALUATIONANSWERTIME);


        String accuracyAnalyzeOther = buffer2.getString(0);
        Integer correctOther = ValueUtil.parseStr2Int(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
        Integer errorOther = ValueUtil.parseStr2Int(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
        Integer sumOther = ValueUtil.parseStr2Int(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
        String submitTimeDate = ValueUtil.parseStr2Str(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUBMITTIMEDATE);
        Long evaluationAnswerTimeOther = ValueUtil.parseStr2Long(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_EVALUATIONANSWERTIME);


        correctMerge += correctOther;
        errorMerge += errorOther;
        sumMerge += sumOther;
        evaluationAnswerTimeMerge += evaluationAnswerTimeOther;


        double accuracy = new BigDecimal(correctMerge).divide(new BigDecimal(sumMerge), 2, BigDecimal.ROUND_HALF_UP).doubleValue();

        buffer1.update(0, "correct=" + correctMerge +
                "|error=" + errorMerge +
                "|sum=" + sumMerge +
                "|accuracy=" + accuracy +
                "|submitTimeDate=" + submitTimeDate +
                "|evaluationAnswerTime=" + evaluationAnswerTimeMerge);

    }

    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }
}
