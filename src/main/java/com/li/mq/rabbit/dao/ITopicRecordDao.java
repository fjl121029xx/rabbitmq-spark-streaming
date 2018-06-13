package com.li.mq.rabbit.dao;

import com.li.mq.rabbit.bean.TopicRecordEntity;

import java.util.List;

public interface ITopicRecordDao {

        void insertBatch(List<TopicRecordEntity> topicRecords);
}
