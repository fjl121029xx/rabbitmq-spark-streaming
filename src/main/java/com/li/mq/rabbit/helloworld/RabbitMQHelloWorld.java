package com.li.mq.rabbit.helloworld;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class RabbitMQHelloWorld {
    public final static String QUEUE_NAME = "rabbitMQ.test";

    /**
     * RabbitMQ消费者
     */
    public static void main(String[] args) throws IOException {

        // 创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ地址
        factory.setHost("192.168.65.129");
        factory.setUsername("admin");
        factory.setPassword("admin");

        //创建一个新的连接
        Connection connection = factory.newConnection();
        //创建一个通道
        Channel channel = connection.createChannel();
        //声明要关注的队列
        /**
         * 队列名称
         * 是否持久化
         * 是否独占队列
         * 当全部消费者客户端链接断开时是否自动删除队列
         * 队列其他参数
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("Customer Waiting Received messages");

        //DefaultConsumer类实现了Consumer接口，通过传入一个频道，
        // 告诉服务器我们需要那个频道的消息，如果频道中有消息，就会执行回调函数handleDelivery
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Customer Received '" + message + "'");
            }
        };

        //自动回复队列应答 -- RabbitMQ中的消息确认机制
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }

    /**
     * RabbitMQ生产者
     */
    @Test
    public void rabbitMQProducer() throws IOException {

        //创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ相关信息
        factory.setHost("192.168.65.129");
        factory.setUsername("admin");
        factory.setPassword("admin");
//        factory.setPort(15672);


        //创建一个新的连接
        Connection connection = factory.newConnection();
        //创建一个通道
        Channel channel = connection.createChannel();
        //  声明一个队列
        /**
         * 队列名称
         * 是否持久化
         * 是否独占队列
         * 当全部消费者客户端链接断开时是否自动删除队列
         * 队列其他参数
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "Hello RabbitMQ";
        //发送消息到队列中
        /**
         * 交换机名称
         * 队列映射的路由key
         * 消息其他属性
         * 发送消息的主题
         */
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));


        System.out.println("Producer Send +'" + message + "'");
        //关闭通道和连接
        channel.close();
        connection.close();
    }
}
