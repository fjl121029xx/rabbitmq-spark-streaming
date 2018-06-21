package scala.ss

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.scalautils.ValueScalaUtil



object TopicRecordCorrectAnalyze {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.checkpoint("D:\\tmp\\checkpoint")


    val mqlines = ssc.receiverStream(new RabbitmqReceiver(ssc, "192.168.65.130", 5672, "admin", "admin"))

    val userId2Info = mqlines.map((s: String) => {

      val arr = s.split("\\|")

      val userId = arr(0).split("=")(1)

      (userId, s)
    })

    val newUpdateFunc = (seq: Seq[String], last: Option[String]) => {

      var tmp: String = "";
      if (last.isDefined) {

        tmp = last.get
      }

      val seqIte = seq.iterator
      while (seqIte.hasNext) {

        tmp += seqIte.next() + "&&"
      }

      Option(tmp)
    }

    var userInfos = userId2Info.updateStateByKey(newUpdateFunc)
      .flatMap(t => {
        t._2.split("\\&\\&").iterator
      })


    val userInfoRows = userInfos.transform((from: RDD[String]) => {

      val userInfoRow = from.map((info: String) => {

        RowFactory.create(
          ValueScalaUtil.parseVal2Str(info, "userId"),
          ValueScalaUtil.parseVal2Str(info, "questionId"),
          ValueScalaUtil.parseVal2Str(info, "correct"),
          ValueScalaUtil.parseVal2Str(info, "time"),
          ValueScalaUtil.parseVal2Str(info, "questionSource"),
          ValueScalaUtil.parseVal2Str(info, "courseWareId"),
          ValueScalaUtil.parseVal2Str(info, "courseWareType"),
          ValueScalaUtil.parseVal2Str(info, "step"),
          ValueScalaUtil.parseVal2Str(info, "subjectId"),
          ValueScalaUtil.parseVal2Str(info, "knowledgePoint"),
          ValueScalaUtil.parseVal2Date(info, "submitTime"))
      })

      val schema = StructType(Array(
        StructField("userId", DataTypes.StringType, true),
        StructField("questionId", DataTypes.StringType, true),
        StructField("correct", DataTypes.StringType, true),
        StructField("time", DataTypes.StringType, true),
        StructField("questionSource", DataTypes.StringType, true),
        StructField("courseWareId", DataTypes.StringType, true),
        StructField("courseWareType", DataTypes.StringType, true),
        StructField("step", DataTypes.StringType, true),
        StructField("subjectId", DataTypes.StringType, true),
        StructField("knowledgePoint", DataTypes.StringType, true),
        StructField("submitTimeDate", DataTypes.StringType, true)
      ))

      val sqlContext = new SQLContext(from.context)

      val userInfodf = sqlContext.createDataFrame(userInfoRow, schema)
      userInfodf.createOrReplaceTempView("tb_topic_record")

      userInfodf.rdd
    })

    userInfoRows.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
