package com.li.mq.dao;


import com.li.mq.bean.AccuracyEntity;

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
