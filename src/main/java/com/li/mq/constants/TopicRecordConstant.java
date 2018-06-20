package com.li.mq.constants;

/**
 * 接口字段常量
 */
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
     * 阶段
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_STEP = "step";

    /**
     * subject_id
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_SUBJECTID = "subjectId";

    /**
     * knowledgePoint   知识点id，逗号分隔	//1,2,3 一级知识点，二级知识点，三机知识点
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_KNOWLEDGEPOINT = "knowledgePoint";

    /**
     * knowledgeLevel   知识点等级   			//查询用
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_KNOWLEDGELEVEL = "knowledgeLevel";

    /**
     * questionSource   视频来源		课后题：1，课中题：2
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_QUESTIONSOURCE = "questionSource";

    /**
     * courseWareId   课件id
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_COURSEWAREID = "courseWareId";
    /**
     * courseWareType   课件类型
     */
    public static final String SSTREAM_TOPIC_RECORD_FIELD_COURSEWARETYPE = "courseWareType";

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

    /**
     * udaf param  averageAnswerTime
     */
    public static final String SSTREAM_TOPIC_RECORD_UDAF_AVERAGEANSWERTIME = "averageAnswerTime";

    /**
     * udaf param  courseWareId
     */
    public static final String SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID = "courseWareId";

    /**
     * udaf param  questionIds
     */
    public static final String SSTREAM_TOPIC_RECORD_UDAF_QUESTIONIDS = "questionIds";

    /**
     * udaf param  knowledgePoint
     */
    public static final String SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT = "knowledgePoint";


}
