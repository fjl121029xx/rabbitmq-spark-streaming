package com.li.mq.bean;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
public class AccuracyEntity {

    public static final String HBASE_TABLE = "topic_record_accuracy_analyze";

    public static final String HBASE_TABLE_FAMILY_COLUMNS = "accuracy_result";

    public static final String HBASE_TABLE_COLUMN_CORRECT = "correct";
    public static final String HBASE_TABLE_COLUMN_ERROR = "error";
    public static final String HBASE_TABLE_COLUMN_SUM = "sum";
    public static final String HBASE_TABLE_COLUMN_ACCURACY = "accuracy";
    public static final String HBASE_TABLE_COLUMN_SUBMITTIME = "submitTime";
    /**
     * 平均答题时间
     */
    public static final String HBASE_TABLE_COLUMN_AVERAGEANSWERTIME = "averageAnswerTime";

    public static final String HBASE_TABLE_FAMILY_COLUMNS2 = "courseware_correct_analyze";
    public static final String HBASE_TABLE_COLUMN_COURSEWARECORRECTANALYZE = "courseWareCorrectAnalyze";
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
     * 知识点答题正确率
     * courseWareId=0|questionIds=0|accuracy=0&&courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0&&courseWareId=0|questionIds=0|accuracy=0&&courseWareId=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0
     */
    private String courseCorrectAnalyze;
}
