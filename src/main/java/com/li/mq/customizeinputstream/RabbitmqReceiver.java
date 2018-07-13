package com.li.mq.customizeinputstream;

import com.alibaba.fastjson.JSONArray;
import com.li.mq.bean.TopicRecordBean;
import com.rabbitmq.client.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class RabbitmqReceiver extends Receiver<String> {

    private final static String QUEUE_NAME = "spark_topic_record";

    private ConnectionFactory factory;
    QueueingConsumer consumer;
    private final static String HOSTNAME = "192.168.100.21";//define hostname for rabbitmq reciver
    private final static String USERNAME = "rabbitmq_ztk";
    private final static String PASSWORD = "rabbitmq_ztk";

    Connection connection;
    Channel channel;

    private static final Log logger = LogFactory.getLog(RabbitmqReceiver.class);

    public RabbitmqReceiver() {

        super(StorageLevel.MEMORY_AND_DISK_2());
    }


    @Override
    public void onStart() {

        new Thread(() -> {
            try {
                receive();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    @Override
    public void onStop() {

    }


    private void receive() throws Exception {

        //创建一个RABBITMQ连接工程
        factory = new ConnectionFactory();
        //设置RabbitMQ地址
        factory.setHost(RabbitmqReceiver.HOSTNAME);
        factory.setUsername(RabbitmqReceiver.USERNAME);
        factory.setPassword(RabbitmqReceiver.PASSWORD);

        //从工厂中获取一个连接
        connection = factory.newConnection();
        //创建一个通道
        channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("Customer Waiting Received messages");


        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");

//                System.out.println(message);
                JSONArray trs = JSONObject.parseArray(message);

                for (int i = 0; i < trs.size(); i++) {
                    JSONObject jsonObject = trs.getJSONObject(i);
                    TopicRecordBean tr = JSONObject.parseObject(jsonObject.toString(), TopicRecordBean.class);
                    logger.info(tr.toString2());
                    store(tr.toString());
                }


            }
        };


        //自动回复队列应答 -- RabbitMQ中的消息确认机制
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}
