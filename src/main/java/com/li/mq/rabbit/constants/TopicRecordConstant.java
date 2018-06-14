package com.li.mq.rabbit.constants;

public class TopicRecordConstant {

    /**
     * questionId   试题Id
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_QUESTIONID = "questionId";

    /**
     * userId   用户id
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_USERID = "userId";

    /**
     * time 做题时长
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_TIME = "time";

    /**
     * correct  是否正确		正确：1，错误：2，未答题：0
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_CORRECT = "correct";

    /**
     * knowledgePoint   所属知识点
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_KNOWLEDGEPOINT = "knowledgePoint";

    /**
     * questionSource   视频来源		课后题：1，课中题：2
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_QUESTIONSOURCE = "questionSource";

    /**
     * course_ware_id   课件id
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_COURSEWAREID = "courseWareId";

    /**
     * submitTime   提交时间
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_SUBMITTIME = "submitTime";

    /**
     * udaf param  correct
     */
    public static final String SSTREAM_TOPIC_RECORD_UDAF_CORRECT = "correct";

    /**
     * udaf param  error
     */
    public static final String SSTREAM_TOPIC_RECORD_UDAF_ERROR = "error";

    /**
     * udaf param  sum
     */
    public static final String SSTREAM_TOPIC_RECORD_UDAF_SUM = "sum";

    /**
     * udaf param  accuracy
     */
    public static final String SSTREAM_TOPIC_RECORD_UDAF_ACCURACY = "accuracy";

    /**
     * udaf param  submitTimeDate
     */
    public static final String SSTREAM_TOPIC_RECORD_UDAF_SUBMITTIMEDATE = "submitTimeDate";


}
