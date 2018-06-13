package com.li.mq.rabbit.dao.impl;

import com.li.mq.rabbit.bean.TopicRecordEntity;
import com.li.mq.rabbit.dao.ITopicRecordDao;
import com.li.mq.rabbit.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class TopicRecordDaoImpl implements ITopicRecordDao {


    @Override
    public void insertBatch(List<TopicRecordEntity> topicRecords) {

        String sql = "insert into topic_record values(?,?,?,?,?,?,?,?)";

        List<Object[]> paramsList = new ArrayList<>();

        for (TopicRecordEntity tr : topicRecords) {
            Object[] params = new Object[]{
                    tr.getUserId(),
                    tr.getQuestionId(),
                    tr.getTime(),
                    tr.getCorrect(),
                    tr.getKnowledgePoint(),
                    tr.getQuestionSource(),
                    tr.getCourseWareId(),
                    tr.getSubmitTime()
            };

            paramsList.add(params);

            JDBCHelper jdbc = JDBCHelper.getInstance();
            jdbc.executeBatch(sql, paramsList);
        }
    }
}
