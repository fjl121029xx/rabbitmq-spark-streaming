package com.li.mq.dao.impl;

import com.li.mq.bean.AccuracyEntity;
import com.li.mq.dao.IAccuracyDao;
import com.li.mq.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AccurayDaoImpl implements IAccuracyDao {


    @Override
    public void insertBatch(List<AccuracyEntity> accuracies) {

        final List<AccuracyEntity> need2update = new ArrayList<>();

        List<AccuracyEntity> accuracyHad = this.findAll();

        final String batchInsertSql = "replace into tb_accuracy(id_time,user_id,correct,error,num,accuracy,submit_time,evaluation_answer_time) values(?,?,?,?,?,?,?,?)";

        batchSql(accuracies, batchInsertSql);
    }

    private void batchSql(List<AccuracyEntity> need2insert, String batchInsertSql) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                List<Object[]> paramsList = new ArrayList<>();

                for (AccuracyEntity ac : need2insert) {
                    Object[] params = new Object[]{
                            ac.getUserId() + ":" + ac.getSubmitTime(),
                            ac.getUserId(),
                            ac.getCorrect(),
                            ac.getError(),
                            ac.getSum(),
                            ac.getAccuracy(),
                            ac.getSubmitTime(),
                            ac.getAverageAnswerTime()
                    };

                    paramsList.add(params);

                    JDBCHelper jdbc = JDBCHelper.getInstance();
                    jdbc.executeBatch(batchInsertSql, paramsList);
                }
            }
        }).start();
    }

    @Override
    public List<AccuracyEntity> findAll() {

        String sql = "select distinct * from tb_accuracy where DATE_FORMAT(now(),'%Y-%m-%d') = submit_time ";

        final List<AccuracyEntity> accuracies = new ArrayList<AccuracyEntity>();

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {


            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
                    long userid = Long.valueOf(String.valueOf(rs.getInt(1)));

                    AccuracyEntity accuracy = new AccuracyEntity();
                    accuracy.setUserId(userid);

                    accuracies.add(accuracy);
                }
            }

        });

        return accuracies;
    }


}
