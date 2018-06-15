package com.li.mq.dao.factory;

import com.li.mq.dao.IAccuracyDao;
import com.li.mq.dao.ITopicRecordDao;
import com.li.mq.dao.impl.AccurayDaoImpl;
import com.li.mq.dao.impl.TopicRecordDaoImpl;

public class DaoFactory {

    public static ITopicRecordDao getITopicRecordDao() {

        return new TopicRecordDaoImpl();
    }

    public static IAccuracyDao getIAccuracyDao() {

        return new AccurayDaoImpl();
    }
}
