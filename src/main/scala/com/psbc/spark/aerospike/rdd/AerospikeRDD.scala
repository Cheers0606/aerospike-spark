package com.psbc.spark.aerospike.rdd

import com.aerospike.client.cluster.Node
import com.aerospike.client.query.{Filter, PredExp, RecordSet, Statement}
import com.aerospike.client.{AerospikeClient, Value}
import com.psbc.spark.aerospike.AqlParser
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import java.util.Calendar
import scala.collection.JavaConverters._


class AerospikeRDD(
                    @transient override val sc: SparkContext,
                    @transient override val aerospikeHosts: Array[Node],
                    val namespace: String,
                    val set: String,
                    val bins: Seq[String],
                    val filterType: AeroFilterType,
                    val filterBin: String,
                    val filterStringVal: String,
                    val lutStart: Calendar,
                    val lutEnd: Calendar,
                    @transient override val filterVals: Seq[(Long, Long)],
                    val attrs: Seq[(SparkFilterType, String, String, Seq[(Long, Long)])] = Seq(),
                    val sch: StructType = null,
                    useUdfWithoutIndexQuery: Boolean = false
                  ) extends BaseAerospikeRDD(sc, aerospikeHosts, filterVals) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    val stmt = new Statement()
    stmt.setNamespace(namespace)
    stmt.setSetName(set)
    stmt.setBinNames(bins: _*)

    val aeroFilter: Option[Filter] = filterType match {
      case FilterNone => None
      case FilterString => Some(Filter.equal(filterBin, filterStringVal))
      case FilterLong => Some(Filter.equal(filterBin, partition.startRange))
      case FilterRange => Some(Filter.range(filterBin, partition.startRange, partition.endRange))
    }

    aeroFilter map (f => stmt.setFilter(f))

    val useUDF = attrs.length > 0 && (useUdfWithoutIndexQuery || aeroFilter.isDefined)

    if (useUDF) {
      var udfFilters: Array[Value] = Array(Value.get(bins.asJava))
      attrs.foreach {
        case (FilterString, s, stringVal, Seq((_, _))) => udfFilters = udfFilters :+ Value.get(Array(Value.get(1), Value.get(s), Value.get(stringVal)))
        case (FilterLong, s, _, Seq((longLower, _))) => udfFilters = udfFilters :+ Value.get(Array(Value.get(2), Value.get(s), Value.get(longLower)))
        case (FilterRange, s, _, Seq((longLower, longUpper))) => udfFilters = udfFilters :+ Value.get(Array(Value.get(3), Value.get(s), Value.get(longLower), Value.get(longUpper)))
        case (FilterIn, s, stringVal, Seq((longLower, _))) =>
          udfFilters =
            if (longLower == 0L)
              udfFilters :+ Value.get(Array(Value.get(4), Value.get(s)) ++ stringVal.split("'").map(Value.get))
            else
              udfFilters :+ Value.get(Array(Value.get(4), Value.get(s)) ++ stringVal.split("'").map(_.toLong).map(Value.get))
      }

      stmt.setAggregateFunction("spark_filters", "multifilter", udfFilters: _*)
      println("UDF Filters applied: " + udfFilters.mkString(","))
    }

    val endpoint = partition.endpoint
    logInfo("RDD: " + split.index + ", Connecting to: " + endpoint._1)

    val client = new AerospikeClient(null, endpoint._1, endpoint._2)
    if (lutStart != null && lutEnd != null) {
      logInfo("Query Predicate: Record updated between " + lutStart + "," + lutEnd)
      stmt.setPredExp(PredExp.recLastUpdate,
        PredExp.integerValue(lutStart),
        PredExp.integerGreaterEq,
        PredExp.recLastUpdate,
        PredExp.integerValue(lutEnd),
        PredExp.integerLess,
        PredExp.and(2)
      )
    } else if (lutStart != null && lutEnd == null) {
      logInfo("Query Predicate: Record updated after(>=) " + lutStart)
      stmt.setPredExp(PredExp.recLastUpdate,
        PredExp.integerValue(lutStart),
        PredExp.integerGreaterEq
      )
    } else if (lutStart == null && lutEnd != null) {
      logInfo("Query Predicate: Record updated before(<) " + lutEnd)
      stmt.setPredExp(PredExp.recLastUpdate,
        PredExp.integerValue(lutEnd),
        PredExp.integerLess
      )
    }
    val res: RecordSet = client.queryNode(null, stmt, client.getNode(endpoint._3))

    context.addTaskCompletionListener(context => {
      res.close();
      client.close()
    })

    res.iterator.asScala.map { rs =>
      if (!useUDF) {
        val pk: String = rs.key.userKey.toString
        Row.fromSeq(Seq.concat(bins.map(rs.record.bins.get(_)), Seq(pk)))
      }
      else {
        rs.record.bins.get("SUCCESS") match {
          case m: java.util.HashMap[Long, Any] =>
            Row.fromSeq(m.asScala.map { f =>
              if (checkType(f._1))
                f._2
              else
                f._2.asInstanceOf[java.lang.Long].intValue
            }.toSeq
            )
          case _ =>
            println("useUDF: " + useUDF)
            println("UDF params: " + attrs.mkString("-"))
            throw new Exception(rs.toString)
        }
      }
    }
  }

  def checkType(position: Long): Boolean = {
    val binName = bins(position.toInt - 1)
    sch(binName).dataType.typeName != "integer"
  }
}

@deprecated("use AqlParser instead")
object AerospikeRDD {

  def removeDoubleSpaces(s: String): String = AqlParser.removeDoubleSpaces(s)

  /**
   *
   * @param asql_statement                 ASQL statement to parse, select only
   * @param numPartitionsPerServerForRange number partitions per Aerospike snode
   * @return namespace, set, bins, filterType, filterBin, filterVals, filterStringVal
   */
  def parseSelect(asql_statement: String, numPartitionsPerServerForRange: Int): (String, String, Seq[String], FilterType, String, Seq[(Long, Long)], String) =
    AqlParser.parseSelect(asql_statement, numPartitionsPerServerForRange).toArray()
}

class QueryParams(namespace: String, set: String, bins: Array[String],
                  filterType: AeroFilterType, filterBin: String, filterVals: Seq[(Long, Long)],
                  filterStringVal: String) {

  @deprecated("use the object")
  def toArray(): (String, String, Seq[String], AeroFilterType, String, Seq[(Long, Long)], String) = {
    (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal)
  }

}
