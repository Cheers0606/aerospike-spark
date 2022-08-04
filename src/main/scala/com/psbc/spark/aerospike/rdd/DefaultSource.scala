package com.psbc.spark.aerospike.rdd

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

class DefaultSource extends RelationProvider {

  /**
   * Creates a new relation for Aerospike select statement.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    val partitionsPerServerPresent: Boolean = parameters.contains("partitionsPerServer") && !parameters("partitionsPerServer").isEmpty
    val useUdfWithoutIndexQueryPresent: Boolean = parameters.contains("useUdfWithoutIndexQuery") && !parameters("useUdfWithoutIndexQuery").isEmpty
    val useUdfWithoutIndexQuery = if (useUdfWithoutIndexQueryPresent) parameters("useUdfWithoutIndexQuery") == "true" else false
    if(partitionsPerServerPresent)
      AeroRelation(parameters("initialHost"), parameters("select"), parameters("partitionsPerServer").toInt, useUdfWithoutIndexQuery)(sqlContext)
    else
      AeroRelation(parameters("initialHost"), parameters("select"), 1, useUdfWithoutIndexQuery)(sqlContext)
  }
}