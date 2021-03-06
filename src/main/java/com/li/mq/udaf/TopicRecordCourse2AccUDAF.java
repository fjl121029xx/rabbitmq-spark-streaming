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
                DataTypes.createStructField("question_id", DataTypes.LongType, true)
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
        buffer.update(0, "courseWareId=0_1_2|correct=|cannot=|undo=|error=|sum=|accuracy=0.00");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        long courseWareIdRow = input.getLong(0);
        Integer courseWareTypeRow = input.getInt(1);
        int correctRow = input.getInt(2);
        int questionSourceRow = input.getInt(3);
        Long questionIdRow = input.getLong(4);

        String courseCorrectAnalyzeInfo = buffer.getString(0);

        String[] courseCorrectAnalyzeInfoArr = courseCorrectAnalyzeInfo.split("\\&\\&");

        boolean notHave = false;

        StringBuilder sb = new StringBuilder();

        String courseWareId = "";
        StringBuilder correctStr = new StringBuilder();
        StringBuilder errorStr = new StringBuilder();

        String correct = "";
        String error = "";
        String undo = "";
        String cannot = "";

        long sum = 0L;
        double accuracy = 0.00;
        String str = "";

        for (int i = 0; i < courseCorrectAnalyzeInfoArr.length; i++) {

            str = courseCorrectAnalyzeInfoArr[i];
            //courseWareId=0_1
            courseWareId = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);

            correct = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            error = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            undo = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_UNDO);
            cannot = ValueUtil.parseStr2Str(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CANNOTANSWER);

            sum = ValueUtil.parseStr2Long(str, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);


            if (courseWareId.equals(courseWareIdRow + "_" + courseWareTypeRow.toString() + "_" + questionSourceRow)) {

                notHave = true;

                if (correctRow == 1) {
                    //作对
                    if (!correct.contains(questionIdRow.toString())) {
                        if (correct.equals("")) {
                            correct += "" + questionIdRow;
                        } else {
                            correct += "," + questionIdRow;
                        }

                    }
                    if (error.contains(questionIdRow + "")) {

                        if (error.startsWith(questionIdRow + ",")) {

                            error = error.replace(questionIdRow + ",", "");
                        } else if (error.endsWith("," + questionIdRow)) {

                            error = error.replace("," + questionIdRow, "");
                        } else if (error.startsWith(questionIdRow + "")) {

                            error = error.replace(questionIdRow + "", "");
                        } else if (error.contains("," + questionIdRow + ",")) {

                            error = error.replace("," + questionIdRow + ",", ",");
                        }
                    }
                    if (undo.contains(questionIdRow + "")) {

                        if (undo.startsWith(questionIdRow + ",")) {

                            undo = undo.replace(questionIdRow + ",", "");
                        } else if (undo.endsWith("," + questionIdRow)) {

                            undo = undo.replace("," + questionIdRow, "");
                        } else if (undo.startsWith(questionIdRow + "")) {

                            undo = undo.replace(questionIdRow + "", "");
                        } else if (undo.contains("," + questionIdRow + ",")) {

                            undo = undo.replace("," + questionIdRow + ",", ",");
                        }
                    }
                    if (cannot.contains(questionIdRow + "")) {

                        if (cannot.startsWith(questionIdRow + ",")) {

                            cannot = cannot.replace(questionIdRow + ",", "");
                        } else if (cannot.endsWith("," + questionIdRow)) {

                            cannot = cannot.replace("," + questionIdRow, "");
                        } else if (cannot.startsWith(questionIdRow + "")) {

                            cannot = cannot.replace(questionIdRow + "", "");
                        } else if (cannot.contains("," + questionIdRow + ",")) {

                            cannot = cannot.replace("," + questionIdRow + ",", ",");
                        }
                    }

                } else if (correctRow == 2) {
                    //做错
                    if (!error.contains(questionIdRow + "")) {

                        if (error.equals("")) {
                            error += "" + questionIdRow;
                        } else {
                            error += "," + questionIdRow;
                        }
                    }

                    if (correct.contains(questionIdRow + "")) {

                        if (correct.startsWith(questionIdRow + ",")) {

                            correct = correct.replace(questionIdRow + ",", "");
                        } else if (correct.endsWith("," + questionIdRow)) {

                            correct = correct.replace("," + questionIdRow, "");
                        } else if (correct.startsWith(questionIdRow + "")) {

                            correct = correct.replace(questionIdRow + "", "");
                        } else if (correct.contains("," + questionIdRow + ",")) {

                            correct = correct.replace("," + questionIdRow + ",", ",");
                        }
                    }
                    if (undo.contains(questionIdRow + "")) {

                        if (undo.startsWith(questionIdRow + ",")) {

                            undo = undo.replace(questionIdRow + ",", "");
                        } else if (undo.endsWith("," + questionIdRow)) {

                            undo = undo.replace("," + questionIdRow, "");
                        } else if (undo.startsWith(questionIdRow + "")) {

                            undo = undo.replace(questionIdRow + "", "");
                        } else if (undo.contains("," + questionIdRow + ",")) {

                            undo = undo.replace("," + questionIdRow + ",", ",");
                        }
                    }
                    if (cannot.contains(questionIdRow + "")) {

                        if (cannot.startsWith(questionIdRow + ",")) {

                            cannot = cannot.replace(questionIdRow + ",", "");
                        } else if (cannot.endsWith("," + questionIdRow)) {

                            cannot = cannot.replace("," + questionIdRow, "");
                        } else if (cannot.startsWith(questionIdRow + "")) {

                            cannot = cannot.replace(questionIdRow + "", "");
                        } else if (cannot.contains("," + questionIdRow + ",")) {

                            cannot = undo.replace("," + questionIdRow + ",", ",");
                        }
                    }


                } else if (correctRow == 0) {

                    if (!undo.contains(questionIdRow + "")) {

                        if (undo.equals("")) {
                            undo += "" + questionIdRow;
                        } else {
                            undo += "," + questionIdRow;
                        }
                    }

                    if (correct.contains(questionIdRow + "")) {

                        if (correct.startsWith(questionIdRow + ",")) {

                            correct = correct.replace(questionIdRow + ",", "");
                        } else if (correct.endsWith("," + questionIdRow)) {

                            correct = correct.replace("," + questionIdRow, "");
                        } else if (correct.startsWith(questionIdRow + "")) {

                            correct = correct.replace(questionIdRow + "", "");
                        } else if (correct.contains("," + questionIdRow + ",")) {

                            correct = correct.replace("," + questionIdRow + ",", ",");
                        }
                    }
                    if (error.contains(questionIdRow + "")) {

                        if (error.startsWith(questionIdRow + ",")) {

                            error = error.replace(questionIdRow + ",", "");
                        } else if (error.endsWith("," + questionIdRow)) {

                            error = error.replace("," + questionIdRow, "");
                        } else if (error.startsWith(questionIdRow + "")) {

                            error = error.replace(questionIdRow + "", "");
                        } else if (error.contains("," + questionIdRow + ",")) {

                            error = error.replace("," + questionIdRow + ",", ",");
                        }
                    }
                    if (cannot.contains(questionIdRow + "")) {

                        if (cannot.startsWith(questionIdRow + ",")) {

                            cannot = cannot.replace(questionIdRow + ",", "");
                        } else if (cannot.endsWith("," + questionIdRow)) {

                            cannot = cannot.replace("," + questionIdRow, "");
                        } else if (cannot.startsWith(questionIdRow + "")) {

                            cannot = cannot.replace(questionIdRow + "", "");
                        } else if (cannot.contains("," + questionIdRow + ",")) {

                            cannot = cannot.replace("," + questionIdRow + ",", ",");
                        }
                    }

                } else if (correctRow == 3) {

                    if (!cannot.contains(questionIdRow + "")) {

                        if (cannot.equals("")) {
                            cannot += "" + questionIdRow;
                        } else {
                            cannot += "," + questionIdRow;
                        }
                    }

                    if (correct.contains(questionIdRow + "")) {

                        if (correct.startsWith(questionIdRow + ",")) {

                            correct = correct.replace(questionIdRow + ",", "");
                        } else if (correct.endsWith("," + questionIdRow)) {

                            correct = correct.replace("," + questionIdRow, "");
                        } else if (correct.startsWith(questionIdRow + "")) {

                            correct = correct.replace(questionIdRow + "", "");
                        } else if (correct.contains("," + questionIdRow + ",")) {

                            correct = correct.replace("," + questionIdRow + ",", ",");
                        }
                    }
                    if (error.contains(questionIdRow + "")) {

                        if (error.startsWith(questionIdRow + ",")) {

                            error = error.replace(questionIdRow + ",", "");
                        } else if (error.endsWith("," + questionIdRow)) {

                            error = error.replace("," + questionIdRow, "");
                        } else if (error.startsWith(questionIdRow + "")) {

                            error = error.replace(questionIdRow + "", "");
                        } else if (error.contains("," + questionIdRow + ",")) {

                            error = error.replace("," + questionIdRow + ",", ",");
                        }
                    }
                    if (undo.contains(questionIdRow + "")) {

                        if (undo.startsWith(questionIdRow + ",")) {

                            undo = undo.replace(questionIdRow + ",", "");
                        } else if (undo.endsWith("," + questionIdRow)) {

                            undo = undo.replace("," + questionIdRow, "");
                        } else if (undo.startsWith(questionIdRow + "")) {

                            undo = undo.replace(questionIdRow + "", "");
                        } else if (undo.contains("," + questionIdRow + ",")) {

                            undo = undo.replace("," + questionIdRow + ",", ",");
                        }
                    }

                }

            }


            if (!courseWareId.startsWith("0_")) {

                long total = 0L;
                long sumcorr = 0L;

                if (!correct.equals("")) {
                    sumcorr = correct.split(",").length;
                    total += sumcorr;
                }
                if (!error.equals("")) {
                    total += error.split(",").length;
                }
                if (!undo.equals("")) {
                    total += undo.split(",").length;
                }
                if (!cannot.equals("")) {
                    total += cannot.split(",").length;
                }
                sum++;
                if (total != 0) {

                    accuracy = new BigDecimal(sumcorr)
                            .divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP)
                            .doubleValue();
                }
                str = "courseWareId=" + courseWareId + "|" +
                        "correct=" + correct + "|" +
                        "error=" + error + "|" +
                        "undo=" + undo + "|" +
                        "cannot=" + cannot + "|" +
                        "sum=" + sum + "|" +
                        "accuracy=" + accuracy + "";
                sb.append(str).append("&&");
            }


        }

        if (!notHave) {
            courseWareId = courseWareIdRow + "_" + courseWareTypeRow.toString() + "_" + questionSourceRow;

            if (correctRow == 1) {

                correct += questionIdRow + "";
            } else if (correctRow == 2) {

                error = questionIdRow + "";
            } else if (correctRow == 0) {

                undo = questionIdRow + "";
            }
            else if (correctRow == 3) {

                cannot = questionIdRow + "";
            }
            sum = 1;
            accuracy = new BigDecimal(1).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
            str = "courseWareId=" + courseWareId + "|" +
                    "correct=" + correct + "|" +
                    "error=" + error + "|" +
                    "undo=" + undo + "|" +
                    "cannot=" + cannot + "|" +
                    "sum=" + sum + "|" +
                    "accuracy=" + accuracy + "";

            sb.append(str).append("&&");
        }
//        sb.replace(0,sb.indexOf("\\&\\&"),"");


        buffer.update(0, sb.toString());
    }

    private void replactQId(String questionIdRow, String correct) {
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
