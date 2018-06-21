package scala.scalautils

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.control.Breaks

object ValueScalaUtil {

  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {

    val a = parseVal2Date("questionId=236|userId=98|time=550|correct=1|knowledgePoint='8,3,4'|questionSource=2|courseWareId=86|courseWareType=1|submitTime=1529475332514|subjectId=3|step=3", "submitTime")
    print(a)
  }

  implicit def parseVal2Str(str: String, field: String) = {

    val fields = str.toString.split("\\|")
    val loop = new Breaks
    var returnField: String = ""

    loop.breakable {
      for (f <- fields) {

        if (f.startsWith(field.toString)) {

          returnField = (f.split("=")(1)).toString
          loop.break()
        }
      }
    }
    returnField
  }

  implicit def parseVal2Date(str: String, field: String): String = {

    val fields = str.toString.split("\\|")
    val loop = new Breaks
    var returnField: String = "0000-00-00"

    loop.breakable {
      for (f <- fields) {

        if (f.startsWith(field.toString)) {

          returnField = sdf.format(new Date((f.split("=")(1)).toLong)).toString
          loop.break()
        }
      }
    }
    returnField
  }


}
