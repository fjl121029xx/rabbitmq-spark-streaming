package com.li.mq.dao;

import com.li.mq.bean.TopicRecordEntity;

import java.util.List;

public interface ITopicRecordDao {


    /**
     *
     */
    void insertBatch(List<TopicRecordEntity> topicRecords);
}
