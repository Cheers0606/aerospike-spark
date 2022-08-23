package com.psbc.spark

import com.psbc.spark.aerospike.rdd._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.{Calendar, GregorianCalendar}

/**
 *
 * @author Enoch on 2022/8/23
 */
object SparkApp {
  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("aerospike-spark")
      .getOrCreate()
    val beginTime = new GregorianCalendar(2022, Calendar.AUGUST, 22)
    val endTime = new GregorianCalendar(2022, Calendar.AUGUST, 24)
    val aero: DataFrame = AeroContext(sc.sparkContext).aeroSInputWithLut("192.168.33.3:13000",
      "select col1,numcol,create_time from test.testset where numcol between 1 and 100",
      sc.sqlContext ,
      beginTime,
      endTime,
      6)
    aero.createOrReplaceTempView("aero")
    sc.sqlContext.sql("select * from aero ").show()
    sc.sqlContext.sql("select avg(numcol) from aero ").show()
    sc.stop()
  }
}