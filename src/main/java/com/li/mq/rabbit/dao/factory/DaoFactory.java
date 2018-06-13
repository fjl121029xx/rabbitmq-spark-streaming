package com.li.mq.rabbit.dao.factory;

import com.li.mq.rabbit.dao.ITopicRecordDao;
import com.li.mq.rabbit.dao.impl.TopicRecordDaoImpl;

public class DaoFactory {

    public static ITopicRecordDao getITopicRecordDao() {

        return new TopicRecordDaoImpl();
    }
}
