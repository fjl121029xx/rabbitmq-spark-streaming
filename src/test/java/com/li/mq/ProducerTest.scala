package com.li.mq

import java.util.Date

import com.li.mq.utils.{RabbitMQConnHandler, RabbitMQProducer}


object ProducerTest {

  def main(args: Array[String]): Unit = {
    testProducer
  }

  def testProducer = {
    val host = "192.168.65.130"
    val port = 5672
    val username = "admin"
    val passw = "admin"
    val exchangeName = "rabbitMQ.test"

    val mqHandler = new RabbitMQConnHandler(host, port, username, passw)
    val sendChannel = mqHandler.getQueueDeclareChannel(exchangeName)
    val producer = new RabbitMQProducer(sendChannel, exchangeName)


    while (true) {
      val msg = new Date().toString()
      producer.sendQueueMsg(exchangeName, msg)
      println(msg)
      Thread.sleep(500)
    }
    producer.close()
    mqHandler.close()
  }
}
