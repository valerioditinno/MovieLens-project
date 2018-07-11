package main.scala

import org.joda.time.format.DateTimeFormat

object MyUtils {

  val refData:Long = strToTime("2000-01-01 00:00:00")

  val previousYearStart:Long = 1364799600  // 2013-04-01 00:00:00
  val previousYearEnd:Long = 1396249200    // 2014-03-31 00:00:00
  val lastYearStart:Long = 1396335600  // 2014-04-01 00:00:00
  val lastYearEnd:Long = 1427785200    // 2015-03-31 00:00:00

  def strToTime(x: String):Long = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").parseDateTime(x).getMillis()/1000  //Unix time

  /*
  def timeToStr(epochMillis: Long): String =
    DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").print(epochMillis)
  */
}
