package com.li.mq.udaf;

import com.li.mq.constants.TopicRecordConstant;
import com.li.mq.utils.ValueUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class TopicRecordItemNumsUDAF extends UserDefinedAggregateFunction {
    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("questionSource", DataTypes.IntegerType, true)
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

        buffer.update(0, "afterClass=0|middleClass=0");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        String itemNums = buffer.getString(0);

        Integer afterClass = ValueUtil.parseStr2Int(itemNums, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AFTERCLASS);
        Integer middleClass = ValueUtil.parseStr2Int(itemNums, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_MIDDLECLASS);

        int questionSource = input.getInt(0);

        if (questionSource == 1) {

            afterClass++;
        } else if (questionSource == 2) {

            middleClass++;
        }

        String str = "afterClass=" + afterClass + "|middleClass=" + middleClass + "";
        buffer.update(0, str);
    }

    @Override
    public void merge(MutableAggregationBuffer merge, Row row) {

        String itemNumsMerge = merge.getString(0);
        Integer afterClassMerge = ValueUtil.parseStr2Int(itemNumsMerge, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AFTERCLASS);
        Integer middleClassMerge = ValueUtil.parseStr2Int(itemNumsMerge, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_MIDDLECLASS);

        String itemNumsRow = row.getString(0);
        Integer afterClassRow = ValueUtil.parseStr2Int(itemNumsRow, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_AFTERCLASS);
        Integer middleClassRow = ValueUtil.parseStr2Int(itemNumsRow, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_MIDDLECLASS);

        afterClassMerge += afterClassRow;
        middleClassMerge += middleClassRow;

        String str = "afterClass=" + afterClassMerge + "|middleClass=" + middleClassMerge + "";
        merge.update(0, str);
    }

    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }
}
