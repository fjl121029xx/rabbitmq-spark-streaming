package com.li.mq.bean;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.li.mq.constants.TopicRecordConstant;
import com.li.mq.utils.ValueUtil;
import lombok.*;

import java.math.BigDecimal;
import java.util.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
public class AccuracyBean {

    public static final String TEST_HBASE_TABLE = "test_tr_accuracy_analyze2";
    public static final String HBASE_TABLE = "tr_accuracy_analyze";

    /**
     * 答题正确率
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS = "accuracy_result";

    public static final String HBASE_TABLE_COLUMN_CORRECT = "correct";
    public static final String HBASE_TABLE_COLUMN_ERROR = "error";
    public static final String HBASE_TABLE_COLUMN_SUM = "sum";
    public static final String HBASE_TABLE_COLUMN_ACCURACY = "accuracy";
    public static final String HBASE_TABLE_COLUMN_SUBMITTIME = "submitTime";
    public static final String HBASE_TABLE_COLUMN_ITEMNUMS = "itemNums";
    //平均答题时间
    public static final String HBASE_TABLE_COLUMN_AVERAGEANSWERTIME = "averageAnswerTime";

    /**
     * 列族2 courseware_correct_analyze 统计课件正确率
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS2 = "courseware_correct_analyze";
    public static final String HBASE_TABLE_COLUMN_COURSEWARECORRECTANALYZE = "courseWareCorrectAnalyze";

    /**
     * 列族3 courseware_correct_analyze 统计课件正确率
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS3 = "knowledgePoint_correct_analyze";
    public static final String HBASE_TABLE_COLUMN_KNOWLEDGEPOINTCORRECTANALYZE = "knowledgePointCorrectAnalyze";

    /**
     * 列族4 count
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS4 = "other";
    public static final String HBASE_TABLE_COLUMN_COUNT = "count";


    /**
     *
     */
    private Long userId;

    /**
     *
     */
    private Long correct;


    /**
     *
     */
    private Long error;

    /**
     *
     */
    private Long sum;

    /**
     *
     */
    private Double accuracy;

    /**
     *
     */
    private String submitTime;

    /**
     * 平均答题时间
     * averageAnswerTime
     */
    private Long averageAnswerTime;

    /**
     * 课件答题正确率
     * courseWareId=639|correct=36|error=17|sum=53|accuracy=0.68&&
     * courseWareId=383|correct=40|error=25|sum=65|accuracy=0.62&&
     */
    private String courseWareCorrectAnalyze;

    /**
     * 知识点答题正确率
     * knowledgePoint=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0
     */
    private String knowledgePointCorrectAnalyze;

    /**
     * count
     * count(*) group bu
     */
    private Long count;

    /**
     * 知识点正确率
     */
//    private List<AccuracyBean> knowledgePointCorrectAnalyze;

    /**
     * 课件正确率
     */
//    private List<AccuracyBean> courseWareCorrectAnalyze;

    /**
     * 课后课中做题数量
     */
    private String itemNums;


    public static String cw(String old, String now) {

        Map<String, String> mapMerger = new HashMap<>();
        String[] cca1 = old.split("\\&\\&");
        for (String s : cca1) {//大聚合

            String courseWareId = ValueUtil.parseStr2Str(s, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            if (courseWareId.startsWith("0_")) {
                continue;
            }
            mapMerger.put(courseWareId, s);
        }

        Map<String, String> mapRow = new HashMap<>();
        String[] cca2 = now.split("\\&\\&");
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
            String courseWareId = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
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


        return upda.toString();
    }

    public static String kn(String old, String now) {


        //大聚合值
        Map<String, String> mapMerger = new HashMap<>();
        String[] cca1 = old.split("\\&\\&");
        for (String s : cca1) {//大聚合

            String knowledgePoint = ValueUtil.parseStr2Str(s, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            if (knowledgePoint.equals("0_0_'0,0,0'")) {
                continue;
            }
            mapMerger.put(knowledgePoint, s);
        }
        //本次聚合值
        Map<String, String> mapRow = new HashMap<>();
        String[] cca2 = now.split("\\&\\&");
        for (String s2 : cca2) {
            String knowledgePoint = ValueUtil.parseStr2Str(s2, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            if (knowledgePoint.equals("0_0_'0,0,0'")) {
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
            String knowledgePoint = ValueUtil.parseStr2Str(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
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

        return upda.toString();
    }
}
