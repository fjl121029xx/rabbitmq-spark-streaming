package com.li.mq.dao;

import com.li.mq.bean.TopicRecordBean;

import java.util.List;

public interface ITopicRecordDao {


    /**
     *
     */
    void insertBatch(List<TopicRecordBean> topicRecords);
}
