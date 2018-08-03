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
import java.text.ParseException;
import java.util.Arrays;

public class TopicRecordAccuracyUDAF extends UserDefinedAggregateFunction {

    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("correct", DataTypes.IntegerType, true),
                        DataTypes.createStructField("submitTimeDate", DataTypes.StringType, true),
                        DataTypes.createStructField("time", DataTypes.LongType, true),
                        DataTypes.createStructField("courseWare_id", DataTypes.LongType, true),
                        DataTypes.createStructField("courseWare_type", DataTypes.IntegerType, true),
                        DataTypes.createStructField("questionSource", DataTypes.IntegerType, true),
                        DataTypes.createStructField("questionId", DataTypes.LongType, true)
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

        buffer.update(0, "correct=|error=|undo=|cannot=|total=0|accuracy=0.00|submitTimeDate=0000_00_00_00_00_00|averageAnswerTime=000000");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row inputRow) {

        int correct = inputRow.getInt(0);
        String submitTimeDate = inputRow.getString(1);
        long time = inputRow.getLong(2);
        long courseWare_id = inputRow.getLong(3);
        int courseWare_type = inputRow.getInt(4);
        int questionSource = inputRow.getInt(5);
        long questionId = inputRow.getLong(6);

        String answer = courseWare_id + "_" + courseWare_type + "_" + questionSource + "_" + questionId;

        String last = buffer.get(0).toString();

        String correctLast = ValueUtil.parseStr2Str(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
        String errorLast = ValueUtil.parseStr2Str(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
        String notknowLast = ValueUtil.parseStr2Str(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_UNDO);
        String cannotLast = ValueUtil.parseStr2Str(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CANNOTANSWER);

        Integer sumLast = ValueUtil.parseStr2Int(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
        Long evaluationAnswerTime = ValueUtil.parseStr2Long(last, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AVERAGEANSWERTIME);


        sumLast++;
        StringBuilder corrAppend = new StringBuilder(correctLast);
        StringBuilder erroAppend = new StringBuilder(errorLast);
        StringBuilder notAppend = new StringBuilder(notknowLast);
        StringBuilder canAppend = new StringBuilder(cannotLast);

        if (correct == 1) {

            StringBuilder[] sbs = AccuracyBean.answerAnalyze(answer, corrAppend, erroAppend, notAppend, canAppend);

            corrAppend = sbs[0];
            erroAppend = sbs[1];
            notAppend = sbs[2];
            canAppend = sbs[3];
        } else if (correct == 2) {

            StringBuilder[] sbs = AccuracyBean.answerAnalyze(answer, erroAppend, corrAppend, notAppend, canAppend);

            erroAppend = sbs[0];
            corrAppend = sbs[1];
            notAppend = sbs[2];
            canAppend = sbs[3];
        } else if (correct == 0) {

            StringBuilder[] sbs = AccuracyBean.answerAnalyze(answer, notAppend, corrAppend, erroAppend, canAppend);

            notAppend = sbs[0];
            corrAppend = sbs[1];
            erroAppend = sbs[2];
            canAppend = sbs[3];
        } else if (correct == 3) {

            StringBuilder[] sbs = AccuracyBean.answerAnalyze(answer, canAppend, corrAppend, erroAppend, notAppend);

            canAppend = sbs[0];
            corrAppend = sbs[1];
            erroAppend = sbs[2];
            notAppend = sbs[3];
        }

        long total = 0L;
        long corr = 0L;
        if (!corrAppend.toString().equals("")) {
            corr = corrAppend.toString().split(",").length;
            total += corr;
        }
        if (!erroAppend.toString().equals("")) {
            total += erroAppend.toString().split(",").length;
        }
        if (!notAppend.toString().equals("")) {
            total += notAppend.toString().split(",").length;
        }
        if (!canAppend.toString().equals("")) {
            total += canAppend.toString().split(",").length;
        }

        evaluationAnswerTime += time;
        double accuracy = 0.00;
        if (total != 0) {

            accuracy = new BigDecimal(corr)
                    .divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
        }


        buffer.update(0, "correct=" + corrAppend.toString() +
                "|error=" + erroAppend.toString() +
                "|sum=" + sumLast +
                "|undo=" + notAppend.toString() +
                "|cannot=" + canAppend.toString() +
                "|accuracy=" + accuracy +
                "|submitTimeDate=" + submitTimeDate +
                "|averageAnswerTime=" + evaluationAnswerTime);
    }


    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {


        String accuracyAnalyze = buffer1.getString(0);

        String correctMerge = ValueUtil.parseStr2Str(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
        String errorMerge = ValueUtil.parseStr2Str(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
        String notknowMerge = ValueUtil.parseStr2Str(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_UNDO);
        String cannotMerge = ValueUtil.parseStr2Str(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CANNOTANSWER);

        Integer sumMerge = ValueUtil.parseStr2Int(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
        Long evaluationAnswerTimeMerge = ValueUtil.parseStr2Long(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AVERAGEANSWERTIME);
        String submitTimeDate = ValueUtil.parseStr2Str(accuracyAnalyze, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUBMITTIMEDATE);

        String accuracyAnalyzeOther = buffer2.getString(0);

        String correctOther = ValueUtil.parseStr2Str(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
        String errorOther = ValueUtil.parseStr2Str(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
        String notknowOther = ValueUtil.parseStr2Str(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_UNDO);
        String cannotOther = ValueUtil.parseStr2Str(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CANNOTANSWER);

        Integer sumOther = ValueUtil.parseStr2Int(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
        Long evaluationAnswerTimeOther = ValueUtil.parseStr2Long(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AVERAGEANSWERTIME);

        String submitTimeDateOther = ValueUtil.parseStr2Str(accuracyAnalyzeOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUBMITTIMEDATE);

        evaluationAnswerTimeMerge += evaluationAnswerTimeOther;

        StringBuilder cor = new StringBuilder();
        StringBuilder err = new StringBuilder();
        StringBuilder notknow = new StringBuilder();
        StringBuilder cannot = new StringBuilder();

        sumMerge += sumOther;

        try {
            cor = AccuracyBean.mergeAnwser(correctMerge, submitTimeDate, correctOther, submitTimeDateOther, cor);
            err = AccuracyBean.mergeAnwser(errorMerge, submitTimeDate, errorOther, submitTimeDateOther, err);
            notknow = AccuracyBean.mergeAnwser(notknowMerge, submitTimeDate, notknowOther, submitTimeDateOther, notknow);
            cannot = AccuracyBean.mergeAnwser(cannotMerge, submitTimeDate, cannotOther, submitTimeDateOther, cannot);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        long total = 0L;
        long sumcorr = 0L;
        long sumerro = 0L;
        long sumnot = 0L;

        if (!cor.toString().equals("")) {
            sumcorr = cor.toString().split(",").length;
            total += sumcorr;
        }
        if (!err.toString().equals("")) {
            sumerro = err.toString().split(",").length;
            total += sumerro;
        }
        if (!notknow.toString().equals("")) {
            sumnot = notknow.toString().split(",").length;
            total += sumnot;
        }
        if (!cannot.toString().equals("")) {
            sumnot = cannot.toString().split(",").length;
            total += sumnot;
        }


        double accuracy = new BigDecimal(sumcorr).divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP).doubleValue();

        buffer1.update(0, "correct=" + cor.toString() +
                "|error=" + err.toString() +
                "|undo=" + notknow.toString() +
                "|cannot=" + cannot.toString() +
                "|sum=" + sumMerge +
                "|accuracy=" + accuracy +
                "|submitTimeDateOther=" + submitTimeDateOther +
                "|averageAnswerTime=" + evaluationAnswerTimeMerge);

    }


    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }


}
