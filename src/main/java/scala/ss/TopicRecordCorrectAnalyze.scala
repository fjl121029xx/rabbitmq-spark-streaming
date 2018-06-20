package scala.ss

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.scalautils.{TopicRecord, ValueUtil}

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


    userInfos.transform((from: RDD[String]) => {

      val userInfoRow = from.map(info => {
        RowFactory.create(ValueUtil.parseVal2Long(info, "userId"),
          ValueUtil.parseVal2Long(info, "questionId"),
          ValueUtil.parseVal2Int(info, "correct"),
          ValueUtil.parseVal2Long(info, "time"),
          ValueUtil.parseVal2Int(info, "questionSource"),
          ValueUtil.parseVal2Long(info, "courseWareId"),
          ValueUtil.parseVal2Int(info, "courseWareType"),
          ValueUtil.parseVal2Long(info, "step"),
          ValueUtil.parseVal2Long(info, "subject_id"),
          ValueUtil.parseVal2Str(info, "knowledgePoint"),
          ValueUtil.parseVal2Date(info, "submitTimeDate"))
      })

      val schema = DataTypes.createStructType(Array(
        DataTypes.createStructField("userId", DataTypes.LongType, true),
        DataTypes.createStructField("questionId", DataTypes.LongType, true),
        DataTypes.createStructField("correct", DataTypes.IntegerType, true),
        DataTypes.createStructField("time", DataTypes.LongType, true),
        DataTypes.createStructField("questionSource", DataTypes.LongType, true),
        DataTypes.createStructField("courseWareId", DataTypes.LongType, true),
        DataTypes.createStructField("courseWareType", DataTypes.LongType, true),
        DataTypes.createStructField("step", DataTypes.LongType, true),
        DataTypes.createStructField("subject_id", DataTypes.LongType, true),
        DataTypes.createStructField("knowledgePoint", DataTypes.StringType, true),
        DataTypes.createStructField("submitTimeDate", DataTypes.StringType, true)
      ))

      val sqlContext = new SQLContext(from.context)

      val userInfodf = sqlContext.createDataFrame(userInfoRow, schema)
      userInfodf.registerTempTable("tb_topic_record")

      sqlContext.sql("select " +
        "userId,questionId,correct," +
        "time,questionSource,courseWareId," +
        "courseWareType,step,subject_id,knowledgePoint,submitTimeDate from tb_topic_record where userId = 78").rdd
    })

    userInfos.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
