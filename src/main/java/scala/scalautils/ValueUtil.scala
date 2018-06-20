package scala.scalautils

import java.text.SimpleDateFormat
import java.util.Date

object ValueUtil {

  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def parseVal2Long(str: String, field: String): Long = {

    val fields = str.split("\\|")

    val fieldsIte = fields.iterator
    while (fieldsIte.hasNext) {
      var f = fieldsIte.next()

      if (f.startsWith(field)) {
        return Long.unbox(f.split("=")(1))
      }
    }
    0
  }

  def parseVal2Int(str: String, field: String): Int = {

    val fields = str.split("\\|")

    val fieldsIte = fields.iterator
    while (fieldsIte.hasNext) {
      var f = fieldsIte.next()

      if (f.startsWith(field)) {
        return Int.unbox(f.split("=")(1))
      }
    }
    0
  }

  def parseVal2Str(str: String, field: String): String = {

    val fields = str.split("\\|")

    val fieldsIte = fields.iterator
    while (fieldsIte.hasNext) {
      var f = fieldsIte.next()

      if (f.startsWith(field)) {
        return f.split("=")(1)
      }
    }
    ""
  }

  def parseVal2Date(str: String, field: String): String = {

    val fields = str.split("\\|")

    val fieldsIte = fields.iterator
    while (fieldsIte.hasNext) {
      var f = fieldsIte.next()

      if (f.startsWith(field)) {
        return sdf.format(new Date(Long.unbox(f.split("=")(1))))
      }
    }
    "0000-00-00"
  }


}
