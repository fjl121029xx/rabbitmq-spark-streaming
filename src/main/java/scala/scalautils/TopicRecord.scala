package scala.scalautils

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s._
import org.json4s.jackson.JsonMethods._

@JsonIgnoreProperties(ignoreUnknown = true)
case class TopicRecord(
                        val userId: Long,
                        val questionId: Long,
                        val correct: Int,
                        val time: Long,
                        val questionSource: Int,
                        val courseWareId: Long,
                        val courseWareType: Int,
                        val step: Long,
                        val subjectId: Long,
                        val knowledgePoint: String,
                        val submitTime: Long,
                        val submitTimeDate: String
                      ) {

  override def toString: String =
    "questionId=" + questionId +
      "|userId=" + userId +
      "|time=" + time +
      "|correct=" + correct +
      "|knowledgePoint='" + knowledgePoint + '\'' +
      "|questionSource=" + questionSource +
      "|courseWareId=" + courseWareId +
      "|courseWareType=" + courseWareType +
      "|submitTime=" + submitTime +
      "|subjectId=" + subjectId +
      "|step=" + step

}

object TopicRecord {

  def main(args: Array[String]): Unit = {


    val mapper = new ObjectMapper()

    mapper.registerModule(DefaultScalaModule)

    val json = "{\"correct\":\"0\",\"courseWareId\":\"77\",\"courseWareType\":\"1\",\"knowledgePoint\":\"8,10,4\",\"questionId\":\"415\",\"questionSource\":\"2\",\"step\":\"1\",\"subjectId\":\"5\",\"submitTime\":\"1529466553342\",\"time\":\"479\",\"userId\":\"48\"}"


    val obj = mapper.readValue(json, classOf[TopicRecord])

    println(obj.toString)
  }
}
