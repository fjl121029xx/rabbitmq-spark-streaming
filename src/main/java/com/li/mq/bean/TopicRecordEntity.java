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
    private Long knowledgePoint;

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
    private String submitTime;

}
