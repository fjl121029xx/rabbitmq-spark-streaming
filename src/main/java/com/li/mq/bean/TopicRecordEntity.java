package com.li.mq.bean;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@ToString
public class TopicRecordEntity {

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


    @Override
    public String toString() {
        return "" +
                "questionId=" + questionId +
                "|userId=" + userId +
                "|time=" + time +
                "|correct=" + correct +
                "|knowledgePoint='" + knowledgePoint + '\'' +
                "|questionSource=" + questionSource +
                "|courseWareId=" + courseWareId +
                "|courseWareType=" + courseWareType +
                "|submitTime=" + submitTime +
                "|subjectId=" + subjectId +
                "|step=" + step;
    }
}
