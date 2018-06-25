package com.li.mq.bean;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

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
    private String courseCorrectAnalyze;

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
}
