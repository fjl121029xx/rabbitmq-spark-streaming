package scala.ss

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.li.mq.utils.{RabbitMQConnHandler, RabbitMQConsumer}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver

import scala.scalautils.TopicRecord


class RabbitmqReceiver(
                        ssm: StreamingContext,
                        rabbitmqHost: String,
                        rabbitmqPort: Int,
                        rabbitmqUsername: String,
                        rabbitmqPassword: String
                      ) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Serializable {

  private var mqThread: Unit = null;
  private val exchangeName = "rabbitMQ.test"

  val mapper = new ObjectMapper()


  override def onStart(): Unit = {

    mapper.registerModule(DefaultScalaModule)
    mqThread = new Thread(new Runnable {
      override def run(): Unit = {

        receive
      }
    }).start()
  }

  override def onStop(): Unit = {

  }

  private def receive(): Unit = {

    val mqHandler = new RabbitMQConnHandler(rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword)

    var consumerChannel = mqHandler.getQueueDeclareChannel(exchangeName)

    val consumer = new RabbitMQConsumer(consumerChannel, exchangeName)
    while (true) {
      val r = consumer.receiveMessage()
      if (r.isRight) {
        val (msg, deliveryTag) = r.right.get
        if (deliveryTag > 0) {

          val obj = mapper.readValue(msg, classOf[TopicRecord])
          println(obj.toString)
          store(obj.toString)

          consumer.basicAck(deliveryTag)
        } else {
          Thread.sleep(1000)
        }
      } else {
        //报错
        if (!mqHandler.connection.isOpen()) {
          mqHandler.reInitConn
        }
        if (!consumerChannel.isOpen()) {
          consumerChannel = mqHandler.getQueueDeclareChannel(exchangeName)
        }
      }

    }
    consumer.close()
    mqHandler.close()
  }
}
