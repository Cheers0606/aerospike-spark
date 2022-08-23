package com.psbc.spark.aerospike.rdd

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

import java.text.SimpleDateFormat
import java.util.Calendar

class DefaultSource extends RelationProvider {

  /**
   * Creates a new relation for Aerospike select statement.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    val partitionsPerServerPresent: Boolean = parameters.contains("partitionsPerServer") && parameters("partitionsPerServer").nonEmpty
    val lutStartPresent: Boolean = parameters.contains("lutStart") && parameters("lutStart").nonEmpty
    val lutEndPresent: Boolean = parameters.contains("lutStart") && parameters("lutStart").nonEmpty
    val lutStart: Calendar = if (lutStartPresent) {
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(parameters("lutStart")))
      calendar
    } else null
    val lutEnd: Calendar = if (lutEndPresent) {
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(parameters("lutEnd")))
      calendar
    } else null

    val useUdfWithoutIndexQueryPresent: Boolean = parameters.contains("useUdfWithoutIndexQuery") && parameters("useUdfWithoutIndexQuery").nonEmpty
    val useUdfWithoutIndexQuery = if (useUdfWithoutIndexQueryPresent) parameters("useUdfWithoutIndexQuery") == "true" else false
    if (partitionsPerServerPresent)
      AeroRelation(parameters("initialHost"), parameters("select"), parameters("partitionsPerServer").toInt, lutStart, lutEnd, useUdfWithoutIndexQuery)(sqlContext)
    else
      AeroRelation(parameters("initialHost"), parameters("select"), 1, lutStart, lutEnd, useUdfWithoutIndexQuery)(sqlContext)
  }
}