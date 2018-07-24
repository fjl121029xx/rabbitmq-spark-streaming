package com.li.mq.udaf;

import com.li.mq.constants.TopicRecordConstant;
import com.li.mq.utils.ValueUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TopicRecordItemNumsUDAF extends UserDefinedAggregateFunction {
    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("questionSource", DataTypes.IntegerType, true),
                DataTypes.createStructField("courseWare_id", DataTypes.LongType, true),
                DataTypes.createStructField("courseWare_type", DataTypes.IntegerType, true),
                DataTypes.createStructField("questionId", DataTypes.LongType, true)
        ));
    }

    @Override
    public StructType bufferSchema() {

        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("itemNums", DataTypes.StringType, true)
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

    //afterClass=0|middleClass=0
    @Override
    public void initialize(MutableAggregationBuffer buffer) {

        buffer.update(0, "afterClass=|middleClass=");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        String itemNums = buffer.getString(0);

        String afterClass = ValueUtil.parseStr2Str(itemNums, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AFTERCLASS);
        String middleClass = ValueUtil.parseStr2Str(itemNums, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_MIDDLECLASS);

        int questionSource = input.getInt(0);
        long courseWare_id = input.getLong(1);
        int courseWare_type = input.getInt(2);
        long questionId = input.getLong(3);

        StringBuilder af = new StringBuilder(afterClass);
        StringBuilder mi = new StringBuilder(middleClass);

        if (questionSource == 1) {

            if (af.toString().equals("")) {
                af.append(courseWare_id + "_" + courseWare_type + "_" + questionId);
            } else {


                if (!af.toString().contains(courseWare_id + "_" + courseWare_type + "_" + questionId)) {
                    af.append(",").append(courseWare_id + "_" + courseWare_type + "_" + questionId);
                }
            }
        } else if (questionSource == 2) {

            if (mi.toString().equals("")) {
                mi.append(courseWare_id + "_" + courseWare_type + "_" + questionId);
            } else {
                if (!af.toString().contains(courseWare_id + "_" + courseWare_type + "_" + questionId)) {
                    mi.append(",").append(courseWare_id + "_" + courseWare_type + "_" + questionId);
                }
            }
        }

        String str = "afterClass=" + af.toString() + "|middleClass=" + mi.toString() + "";
        buffer.update(0, str);
    }

    @Override
    public void merge(MutableAggregationBuffer merge, Row row) {

        String itemNumsMerge = merge.getString(0);
        String afterClassMerge = ValueUtil.parseStr2Str(itemNumsMerge, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AFTERCLASS);
        String middleClassMerge = ValueUtil.parseStr2Str(itemNumsMerge, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_MIDDLECLASS);

        String itemNumsRow = row.getString(0);
        String afterClassRow = ValueUtil.parseStr2Str(itemNumsRow, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AFTERCLASS);
        String middleClassRow = ValueUtil.parseStr2Str(itemNumsRow, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_MIDDLECLASS);


        List<String> af = new ArrayList<>(Arrays.asList((afterClassMerge + "," + afterClassRow).split(",")));
        StringBuilder saf = new StringBuilder();
        for (String str : af) {
            if (!saf.toString().contains(str)) {
                saf.append(str).append(",");
            }
        }

        List<String> mi = new ArrayList<>(Arrays.asList((middleClassMerge + "," + middleClassRow).split(",")));
        StringBuilder smi = new StringBuilder();
        for (String str : mi) {
            if (!smi.toString().contains(str)) {
                smi.append(str).append(",");
            }
        }


        String str = "afterClass=" + saf.toString() + "|middleClass=" + smi.toString() + "";
        merge.update(0, str);
    }

    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }
}
