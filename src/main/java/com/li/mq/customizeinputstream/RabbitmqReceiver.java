package com.li.mq.customizeinputstream;

import com.rabbitmq.client.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.IOException;

public class RabbitmqReceiver extends Receiver<String> {

    private final static String QUEUE_NAME = "rabbitMQ.test";

    private ConnectionFactory factory;
    QueueingConsumer consumer;
    String hostname = "192.168.65.130";//define hostname for rabbitmq reciver
    Connection connection;
    Channel channel;

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
        factory.setHost("192.168.65.130");
        factory.setUsername("admin");
        factory.setPassword("admin");

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
                store(message);
            }
        };

        //自动回复队列应答 -- RabbitMQ中的消息确认机制
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}
