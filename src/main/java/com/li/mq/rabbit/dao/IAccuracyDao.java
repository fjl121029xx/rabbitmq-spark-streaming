package com.li.mq.rabbit.dao;


import com.li.mq.rabbit.bean.AccuracyEntity;

import java.util.List;

public interface IAccuracyDao {

    /**
     *
     */
    void insertBatch(List<AccuracyEntity> topicRecords);

    /**
     *
     */
    List<AccuracyEntity> findAll();
}
