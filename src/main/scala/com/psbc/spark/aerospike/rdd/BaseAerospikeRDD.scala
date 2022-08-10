package com.psbc.spark.aerospike.rdd

import com.aerospike.client.AerospikeClient
import com.aerospike.client.cluster.Node
import com.aerospike.client.policy.ClientPolicy
import com.psbc.spark.aerospike.AqlParser
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.Filter

import java.net.InetAddress


//Filter types: 0 none, 1 - equalsString, 2 - equalsLong, 3 - range

case class AerospikePartition(index: Int,
                              endpoint: (String, Int, String),
                              startRange: Long,
                              endRange: Long
                               ) extends Partition()


//class AerospikePartitioner(val aerospikeHosts: Array[String]) extends HashPartitioner(aerospikeHosts.length) {
//  override def equals(other: Any): Boolean = other match {
//    case h: AerospikePartitioner => {
//      h.aerospikeHosts.diff(aerospikeHosts).length == 0 && aerospikeHosts.diff(h.aerospikeHosts).length == 0
//    }
//    case _ =>
//      false
//  }
//}

abstract class BaseAerospikeRDD(
                                 @transient val sc: SparkContext,
                                 @transient val aerospikeHosts: Array[Node],
                                 val filterVals: Seq[(Long, Long)]
                                 )
  extends RDD[Row](sc, Seq.empty) with Logging {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(InetAddress.getByName(split.asInstanceOf[AerospikePartition].endpoint._1).getHostName)
  }

  //   override val partitioner: Option[Partitioner] = if (makePartitioner) Some(new AerospikePartitioner(aerospikeHosts.map(n => n.getHost.toString))) else None

  override protected def getPartitions: Array[Partition] = {
    {
      for {i <- 0 until aerospikeHosts.size
           node: Node = aerospikeHosts(i)
           aliases = node.getHost
           (j, k) <- filterVals.zipWithIndex
           resultName =  node.getHost.name
           part = new AerospikePartition(i * filterVals.length + k, (resultName, node.getHost.port, node.getName), j._1, j._2).asInstanceOf[Partition]
      } yield part
    }.toArray
  }
}


class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def aeroInput(
                 initialHost: String,
                 select: String,
                 partitionsPerServer: Int = 1,
 		 timeout: Int = 1000
                 ) = {
    val (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal) = AqlParser.parseSelect(select, partitionsPerServer).toArray()
    var hosts: Array[Node] = null
    val policy = new ClientPolicy()
    val splitHost = initialHost.split(":")
    policy.timeout=timeout
    val client = new AerospikeClient(policy, splitHost(0), splitHost(1).toInt)

    try {
      hosts = client.getNodes
    } finally {
      client.close()
    }
    new AerospikeRDD(sc, hosts, namespace, set, bins, filterType, filterBin, filterStringVal, filterVals)
  }

  def aeroSInput(
                  initialHost: String,
                  select: String,
                  cont: SQLContext,
                  partitionsPerServer: Int = 1
                  ) = {

    val rel = AeroRelation(initialHost, select, partitionsPerServer)(cont)
    val schema = rel.schema
//    cont.applySchema(rel.buildScan(schema.fieldNames.toArray, Array[Filter]()), schema)
    cont.createDataFrame(rel.buildScan(schema.fieldNames, Array[Filter]()), schema)
  }


}

