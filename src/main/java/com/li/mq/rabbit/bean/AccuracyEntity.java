package com.li.mq.rabbit.bean;

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
