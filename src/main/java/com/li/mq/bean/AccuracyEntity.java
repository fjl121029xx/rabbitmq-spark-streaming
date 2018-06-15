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
    public static final String HBASE_TABLE_COLUMN_COREVALUATIONANSWERTIMERECT = "corevaluationAnswerTimerect";
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
     *
     */
    private Long evaluationAnswerTime;
}
