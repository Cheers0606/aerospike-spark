package com.psbc.spark.aerospike

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.language.implicitConversions


package object rdd {
  implicit def AeroContext(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)



  implicit class AeroSqlContext(sqlContext: SQLContext) {
    def aeroRDD(
                 initialHost: String,
                 select: String,
                 numPartitionsPerServerForRange : Int = 1) =
      new SparkContextFunctions(sqlContext.sparkContext).aeroSInput(initialHost, select,sqlContext, numPartitionsPerServerForRange )

  }
}
