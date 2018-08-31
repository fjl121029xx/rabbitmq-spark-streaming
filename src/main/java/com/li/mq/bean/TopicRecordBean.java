package com.li.mq.bean;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.li.mq.constants.TopicRecordConstant;
import com.li.mq.utils.ValueUtil;
import lombok.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@ToString
public class TopicRecordBean {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm");

    private static final SimpleDateFormat sdfYMD = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
    /**
     * 试题Id
     */
    private Long questionId;

    /**
     * 用户id
     */
    private Long userId;

    /**
     * 做题时长
     */
    private Long time;

    /**
     * 是否正确
     * 正确：1，错误：2，未答题：0
     */
    private int correct;

    /**
     * 所属知识点
     */
    private String knowledgePoint;

    /**
     * 视频来源（课中题，课后题）
     * 课后题：1，课中题：2
     */
    private int questionSource;

    /**
     * 课件id
     */
    private Long courseWareId;

    /**
     * 课件类型
     */
    private int courseWareType;

    /**
     * 提交时间
     */
    private Long submitTime;

    /**
     * 提交时间 yyyy-MM-dd
     */
    private String submitTimeDate;

    /**
     * 科目
     */
    private Long subjectId;

    /**
     * 阶段
     */
    private Long step;

    /**
     * 是否听过课
     */
    private Integer listened;


    @Override
    public String toString() {
        return "" +
                "questionId=" + questionId +
                "|userId=" + userId +
                "|time=" + time +
                "|correct=" + correct +
                "|knowledgePoint=" + knowledgePoint +
                "|questionSource=" + questionSource +
                "|courseWareId=" + courseWareId +
                "|courseWareType=" + courseWareType +
                "|submitTime=" + submitTime +
                "|subjectId=" + subjectId +
                "|step=" + step +
                "|listened=" + listened;
    }

    public String toString2() {
        return "topicRecord" +
                "\t" + questionId +
                "\t" + userId +
                "\t" + time +
                "\t" + correct +
                "\t" + knowledgePoint +
                "\t" + questionSource +
                "\t" + courseWareId +
                "\t" + courseWareType +
                "\t" + submitTime +
                "\t" + subjectId +
                "\t" + step +
                "\t" + listened ;
    }

    public static Row getInto2Row(String info) {
        //用户id
        Long userId = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_USERID);
        //课件id
        Long courseWare_id = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_COURSEWAREID);
        //课件类型
        Integer courseWare_type = ValueUtil.parseStr2Int(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_COURSEWARETYPE);
        //试题Id
        Long questionId = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_QUESTIONID);
        //做题时长
        Long time = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_TIME);
        //是否正确
        Integer correct = ValueUtil.parseStr2Int(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_CORRECT);
        //阶段
        Long step = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_STEP);
        //科目
        Long subjectId = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_SUBJECTID);
        //所属知识点
        String knowledgePoint = ValueUtil.parseStr2Str(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_KNOWLEDGEPOINT);
        //视频来源
        Integer questionSource = ValueUtil.parseStr2Int(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_QUESTIONSOURCE);
        //视频来源
        Integer listened = ValueUtil.parseStr2Int(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_LISTENED);
        //提交时间
        Long submitTime = ValueUtil.parseStr2Long(info, TopicRecordConstant.SSTREAM_TOPIC_RECORD_FIELD_SUBMITTIME);
        String submitTimeDate = sdfYMD.format(new Date(submitTime));

        return RowFactory.create(userId,
                courseWare_id,
                courseWare_type,
                questionId,
                time,
                correct,
                step,
                subjectId,
                knowledgePoint,
                questionSource,
                submitTimeDate,
                listened);
    }
}
