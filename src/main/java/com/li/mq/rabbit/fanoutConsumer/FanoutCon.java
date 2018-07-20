package com.li.mq.rabbit.fanoutConsumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

public class FanoutCon {

    private static final String EXCHANGE_NAME = "videoplay_record";
    private static ConnectionFactory factory = new ConnectionFactory();
    private static Connection connection = null;
    private static Channel channel = null;

    static {
        factory.setHost("192.168.100.21");
        factory.setUsername("rabbitmq_ztk");
        factory.setPassword("rabbitmq_ztk");


        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
                    String queueName = "log-fb2";  //队列1名称
                    channel.queueDeclare(queueName, false, false, false, null);
                    channel.queueBind(queueName, EXCHANGE_NAME, "");//把Queue、Exchange绑定
                    QueueingConsumer consumer = new QueueingConsumer(channel);
                    channel.basicConsume(queueName, true, consumer);
                    while (true) {
                        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                        String message = new String(delivery.getBody());
                        System.out.println(" [2] Received '" + message + "'");

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
                    String queueName = "log-fb3";  //队列2名称
                    channel.queueDeclare(queueName, false, false, false, null);
                    channel.queueBind(queueName, EXCHANGE_NAME, "");//把Queue、Exchange绑定
                    QueueingConsumer consumer = new QueueingConsumer(channel);
                    channel.basicConsume(queueName, true, consumer);
                    while (true) {
                        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                        String message = new String(delivery.getBody());
                        System.out.println(" [3] Received '" + message + "'");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

//        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");//声明Exchange
//        for (int i = 0; i <= 2; i++) {
//            String message = "hello word!" + i;
//            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());  //fanout的情况下，队列为默认，
//            System.out.println(" [x] Sent '" + message + "'");
//        }
//        channel.close();
//        connection.close();
    }
}
