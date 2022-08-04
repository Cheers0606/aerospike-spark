package com.psbc.spark.aerospike.rdd

import com.aerospike.client.cluster.Node
import com.aerospike.client.policy.QueryPolicy
import com.aerospike.client.query.{RecordSet, Statement}
import com.aerospike.client.{AerospikeClient, Info}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import java.lang
import scala.collection.JavaConverters._

case class AeroRelation(initialHost: String,
                        select: String,
                        partitionsPerServer: Int = 1,
                        useUdfWithoutIndexQuery: Boolean = false)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  var schemaCache: StructType = StructType(Seq.empty[StructField])
  var nodeList: Array[Node] = null
  var namespaceCache: String = null
  var setCache: String = null
  var filterTypeCache: AeroFilterType = FilterNone
  var filterBinCache: String = null
  var filterValsCache: Seq[(Long, Long)] = null
  var filterStringValCache: String = null

  override def schema: StructType = {

    if (schemaCache.isEmpty && nodeList == null) {
      val (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal) = AqlParser.parseSelect(select, partitionsPerServer).toArray()
      namespaceCache = namespace
      setCache = set
      filterTypeCache = filterType
      filterBinCache = filterBin
      filterValsCache = filterVals
      filterStringValCache = filterStringVal
      val splitHost = initialHost.split(":")
      val client = new AerospikeClient(null, splitHost(0), splitHost(1).toInt)

      try {
        nodeList = client.getNodes

        val stmt = new Statement()
        stmt.setNamespace(namespace)
        stmt.setSetName(set)
        if (bins.length > 1 || bins.head != "*")
          stmt.setBinNames(bins: _*)

        val qp: QueryPolicy = client.queryPolicyDefault
        qp.maxConcurrentNodes = 1
        qp.recordQueueSize = 1

        val recs: RecordSet = client.queryNode(qp, stmt, nodeList.head)
        try {
          val iterator = recs.iterator()

          if (iterator.hasNext) {
            val record = iterator.next().record
            var binNames = bins
            if (bins.length == 1 && bins.head == "*") {
              binNames = record.bins.keySet.asScala.toSeq
            }
            schemaCache = StructType(binNames.map { b =>
              record.bins.get(b) match {
                case v: Integer => StructField(b, IntegerType, nullable = true)
                case v: lang.Long => StructField(b, LongType, nullable = true)
                case _ => StructField(b, StringType, nullable = true)
              }
            })
          }
        } finally {
          recs.close()
        }
      } finally {
        client.close()
      }
    }

    schemaCache
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (schemaCache.isEmpty && nodeList == null) {
      schema //ensure def schema always called before this method
    }

    if (filters.length > 0) {
      val index_info = Info.request(nodeList.head, "sindex").split(";")

      def checkIndex(attr: String): Boolean = {
        index_info
          .filter(_.contains("ns=" + namespaceCache + ":set=" + setCache + ":"))
          .filter(_.contains(":bins=" + attr + ":"))
          .length > 0
      }

      def checkNumeric(attr: String): Boolean = {
        val attrType: String = schemaCache(attr).dataType.typeName
        attrType == "long" || attrType == "integer"
      }

      def tupleGreater(attribute: String, value: Any): Seq[(SparkFilterType, String, String, Seq[(Long, Long)])] = {
        Seq((FilterRange, attribute, "", Seq((value.toString.toLong, filters.flatMap {
          case LessThan(sf_attribute, sf_value) if sf_attribute == attribute => Seq(sf_value.toString.toLong)
          case LessThanOrEqual(sf_attribute, sf_value) if sf_attribute == attribute => Seq(sf_value.toString.toLong)
          case _ => Seq(Long.MaxValue)
        }.min))))
      }

      def tupleLess(attribute: String, value: Any): Seq[(SparkFilterType, String, String, Seq[(Long, Long)])] = {
        Seq((FilterRange, attribute, "", Seq((filters.flatMap {
          case GreaterThan(sf_attribute, sf_value) if sf_attribute == attribute => Seq(sf_value.toString.toLong)
          case GreaterThanOrEqual(sf_attribute, sf_value) if sf_attribute == attribute => Seq(sf_value.toString.toLong)
          case _ => Seq(Long.MinValue)
        }.max, value.toString.toLong))))
      }

      val allFilters: Seq[(SparkFilterType, String, String, Seq[(Long, Long)])] = filters.flatMap {
        case EqualTo(attribute, value) =>
          Seq(
            if (!checkNumeric(attribute))
              (FilterString, attribute, value.toString, Seq((0L, 0L)))
            else
              (FilterLong, attribute, "", Seq((value.toString.toLong, 0L)))
          )

        case GreaterThanOrEqual(attribute, value) if checkNumeric(attribute) =>
          tupleGreater(attribute, value)

        case GreaterThan(attribute, value) if checkNumeric(attribute) =>
          tupleGreater(attribute, value)

        case LessThanOrEqual(attribute, value) if checkNumeric(attribute) =>
          tupleLess(attribute, value)

        case LessThan(attribute, value) if checkNumeric(attribute) =>
          tupleLess(attribute, value)

        case In(attribute, value) =>
          Seq((FilterIn, attribute, value.mkString("'"), Seq((if (checkNumeric(attribute)) 1L else 0L, 0L))))

        case _ => Seq()
      }

      if (allFilters.length > 0) {
        val splitHost = initialHost.split(":")
        val client = new AerospikeClient(null, splitHost(0), splitHost(1).toInt)
        try {
          AerospikeUtils.registerUdfWhenNotExist(client)
        } finally {
          client.close
        }
      }

      val indexedAttrs = allFilters.filter(s => {
        checkIndex(s._2) && s._1.isInstanceOf[AeroFilterType]
      })

      if (filterTypeCache == FilterNone && indexedAttrs.length > 0) {
        //originally declared index query takes priority
        val (filterType: AeroFilterType, filterBin, filterStringVal, filterVals) = indexedAttrs.head
        var tuples: Seq[(Long, Long)] = filterVals
        val lower: Long = filterVals.head._1
        val upper: Long = filterVals.head._2
        val range: Long = upper - lower
        if (partitionsPerServer > 1 && range >= partitionsPerServer) {
          val divided = range / partitionsPerServer
          tuples = (0 until partitionsPerServer)
            .map(i => (lower + divided * i, if (i == partitionsPerServer - 1) upper else lower + divided * (i + 1) - 1))
        }
        new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterType, filterBin, filterStringVal, tuples, allFilters, schemaCache, useUdfWithoutIndexQuery)
      }
      else {
        new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterTypeCache, filterBinCache, filterStringValCache, filterValsCache, allFilters, schemaCache, useUdfWithoutIndexQuery)
      }
    }
    else {
      new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterTypeCache, filterBinCache, filterStringValCache, filterValsCache)
    }
  }
}
