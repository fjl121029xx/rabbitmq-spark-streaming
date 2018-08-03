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
                        DataTypes.createStructField("time", DataTypes.LongType, true),
                        DataTypes.createStructField("questionId", DataTypes.LongType, true)
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
        //knowledgePoint=step_subjectId_knowledgePoint|questionIds=0|correct=0|error=0|sum=0|accuracy=0
        buffer.update(0, "knowledgePoint=0_0_0,0,0|correct=|error=|cannot=|sum=|undo=|accuracy=0|totalTime=0");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        long step = input.getLong(0);
        long subjectId = input.getLong(1);
        String kpRow = input.getString(2);
        int correctRow = input.getInt(3);
        long timeRow = input.getLong(4);
        long questionId = input.getLong(5);

        kpRow = step + "_" + subjectId + "_" + kpRow;


        StringBuilder sb = new StringBuilder();

        double accuracy = 0.00;

        boolean notHave = false;
        String[] KpInfobuffered = buffer.getString(0).split("\\&\\&");
        for (int i = 0; i < KpInfobuffered.length; i++) {

            String strbuffer = KpInfobuffered[i];

            //1_2_1,2,3
            String bufferedPoint = ValueUtil.parseStr2Str(strbuffer, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);

            String correct = ValueUtil.parseStr2Str(strbuffer, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            String error = ValueUtil.parseStr2Str(strbuffer, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            String undo = ValueUtil.parseStr2Str(strbuffer, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_UNDO);
            String cannotAnswer = ValueUtil.parseStr2Str(strbuffer, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CANNOTANSWER);

            long sum = ValueUtil.parseStr2Long(strbuffer, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
            long time = ValueUtil.parseStr2Long(strbuffer, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_TOTALTIME);


            StringBuilder corrAppend = new StringBuilder(correct);
            StringBuilder erroAppend = new StringBuilder(error);
            StringBuilder undoAppend = new StringBuilder(undo);
            StringBuilder cannotAnswerAppend = new StringBuilder(cannotAnswer);

            if (kpRow.equals(bufferedPoint)) {

                notHave = true;

                if (correctRow == 1) {

                    StringBuilder[] sbs = AccuracyBean.answerAnalyze(Long.toString(questionId), corrAppend, erroAppend, undoAppend, cannotAnswerAppend);
                    corrAppend = sbs[0];
                    erroAppend = sbs[1];
                    undoAppend = sbs[2];
                    cannotAnswerAppend = sbs[3];
                } else if (correctRow == 2) {

                    StringBuilder[] sbs = AccuracyBean.answerAnalyze(Long.toString(questionId), erroAppend, corrAppend, undoAppend, cannotAnswerAppend);
                    erroAppend = sbs[0];
                    corrAppend = sbs[1];
                    undoAppend = sbs[2];
                    cannotAnswerAppend = sbs[3];
                } else if (correctRow == 0) {

                    StringBuilder[] sbs = AccuracyBean.answerAnalyze(Long.toString(questionId), undoAppend, corrAppend, erroAppend, cannotAnswerAppend);
                    undoAppend = sbs[0];
                    corrAppend = sbs[1];
                    erroAppend = sbs[2];
                    cannotAnswerAppend = sbs[3];
                } else if (correctRow == 3) {

                    StringBuilder[] sbs = AccuracyBean.answerAnalyze(Long.toString(questionId), cannotAnswerAppend, corrAppend, erroAppend, undoAppend);
                    cannotAnswerAppend = sbs[0];
                    corrAppend = sbs[1];
                    erroAppend = sbs[2];
                    undoAppend = sbs[2];
                }

                sum++;
                time += timeRow;
            }
            //计算正确率
            long total = 0L;
            long corr = 0L;
            if (!corrAppend.toString().equals("")) {
                corr = corrAppend.toString().split(",").length;
                total += corr;
            }
            if (!erroAppend.toString().equals("")) {
                total += erroAppend.toString().split(",").length;
            }
            if (!undoAppend.toString().equals("")) {
                total += undoAppend.toString().split(",").length;
            }
            if (!cannotAnswerAppend.toString().equals("")) {
                total += cannotAnswerAppend.toString().split(",").length;
            }

            if (!bufferedPoint.equals("0_0_0,0,0")) { //去掉knowledgePoint=0

                if (total != 0) {

                    accuracy = new BigDecimal(corr).divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
                }
                strbuffer = "knowledgePoint=" + bufferedPoint + "|" +
                        "correct=" + corrAppend.toString() + "|" +
                        "error=" + erroAppend.toString() + "|" +
                        "undo=" + undoAppend.toString() + "|" +
                        "cannot=" + cannotAnswerAppend.toString() + "|" +
                        "sum=" + sum + "|" +
                        "accuracy=" + accuracy + "|" +
                        "totalTime=" + time;
                sb.append(strbuffer).append("&&");
            }

        }
        String str;
        if (!notHave) {

            String knowledgePoint = kpRow;
            long time = timeRow;
            String correct = "", error = "", notknow = "", cannot = "", sum = "1";
            int correctnum = 0;
            if (correctRow == 1) {
                correctnum = 1;
                correct += questionId;
            } else if (correctRow == 2) {
                error += questionId;
            } else if (correctRow == 0) {

                notknow += questionId;
            } else if (correctRow == 3) {

                cannot += questionId;
            }
            accuracy = new BigDecimal(correctnum).divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP).doubleValue();

            str = "knowledgePoint=" + knowledgePoint + "|" +
                    "correct=" + correct + "|" +
                    "error=" + error + "|" +
                    "undo=" + notknow + "|" +
                    "cannot=" + cannot + "|" +
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
