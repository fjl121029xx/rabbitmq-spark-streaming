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

public class TopicRecordCourse2AccUDAF extends UserDefinedAggregateFunction {

    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(Arrays.asList(

                DataTypes.createStructField("courseWare_id", DataTypes.LongType, true),
                DataTypes.createStructField("courseWare_type", DataTypes.IntegerType, true),
                DataTypes.createStructField("correct", DataTypes.IntegerType, true),
                DataTypes.createStructField("question_source", DataTypes.IntegerType, true),
                DataTypes.createStructField("question_id", DataTypes.LongType, true),
                DataTypes.createStructField("userId", DataTypes.LongType, true)
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
        //(courseWareId)_(courseware_type)_(courseware_id)
        //question_id
        //courseWareId=0|correct=0|error=0|sum=0|accuracy=0
        buffer.update(0, "courseWareId=0_1_2|correct=0|error=0|sum=0|accuracy=0.00");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        long courseWareIdRow = input.getLong(0);
        Integer courseWareTypeRow = input.getInt(1);
        int correctRow = input.getInt(2);
        int questionSourceRow = input.getInt(3);
        long questionIdRow = input.getLong(4);

        String courseCorrectAnalyzeInfo = buffer.getString(0);

        String[] courseCorrectAnalyzeInfoArr = courseCorrectAnalyzeInfo.split("\\&\\&");

        boolean notHave = false;

        StringBuilder sb = new StringBuilder();

        String courseWareId = "";
        StringBuilder correctStr = new StringBuilder();
        StringBuilder errorStr = new StringBuilder();

        String correct = "";
        String error = "";
        long sum = 0L;
        double accuracy = 0.00;
        String str = "";

        for (int i = 0; i < courseCorrectAnalyzeInfoArr.length; i++) {

            str = courseCorrectAnalyzeInfoArr[i];
            //courseWareId=0_1
            courseWareId = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);

            correct = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            error = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            sum = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);

            if (courseWareId.equals(courseWareIdRow + "_" + courseWareTypeRow.toString() + "_" + questionSourceRow)) {

                notHave = true;

                if (correctRow == 0) {
                    //作对
                    if (!correct.contains(questionIdRow + "")) {
                        if (correct.equals("")) {
                            correct += "" + questionIdRow;
                        } else {
                            correct += "," + questionIdRow;
                        }

                    }
                    if (error.contains(questionIdRow + "")) {

                        if (error.startsWith(questionIdRow + "")) {

                            error.replace(questionIdRow + ",", "");
                        } else if (correct.endsWith(questionIdRow + "")) {

                            error.replace("," + questionIdRow + "", "");
                        } else {

                            error.replace("," + questionIdRow + ",", ",");
                        }
                    }


                } else if (correctRow == 1) {
                    //做错
                    if (correct.contains(questionIdRow + "")) {

                        replactQId(questionIdRow, correct);
                    }
                    if (!error.contains(questionIdRow + "")) {

                        if (error.equals("")) {
                            error += "" + questionIdRow;
                        } else {
                            error += "," + questionIdRow;
                        }
                    }

                }
                sum++;
                accuracy = new BigDecimal(correct.split(",").length).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();


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
            courseWareId = courseWareIdRow + "_" + courseWareTypeRow.toString() + "_" + questionSourceRow;

            if (correctRow == 0) {

                correct += questionIdRow + "";
            } else if (correctRow == 1) {

                error = questionIdRow + "";
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

    private void replactQId(long questionIdRow, String correct) {
        if (correct.startsWith(questionIdRow + "")) {

            correct.replace(questionIdRow + ",", "");
        } else if (correct.endsWith(questionIdRow + "")) {

            correct.replace("," + questionIdRow + "", "");
        } else {

            correct.replace("," + questionIdRow + ",", ",");
        }
    }


    //courseWareId=0|accuracy=0&&courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0&&
    //courseWareId=0|accuracy=0&&courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0
    @Override
    public void merge(MutableAggregationBuffer merger, Row row) {

        String cw = AccuracyBean.cw(merger.getString(0), row.getString(0));

        merger.update(0, cw);

    }

    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }
}
