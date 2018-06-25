package com.li.mq.dao;


import com.li.mq.bean.AccuracyBean;

import java.util.List;

public interface IAccuracyDao {

    /**
     *
     */
    void insertBatch(List<AccuracyBean> topicRecords);

    /**
     *
     */
    List<AccuracyBean> findAll();
}
