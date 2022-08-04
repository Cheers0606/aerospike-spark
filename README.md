Aerospike Spark Connector
===============

* 从AQL语句（包括确定单行查询上的数据类型）创建模式RDD，如果不与SparkSQL上下文一起使用，则仅创建RDD[row]
* 并行查询本地Aerospike节点（允许从单个服务器并行读取范围查询）
  
使用例子:

- scala版本
```scala
import com.psbc.spark.aerospike.rdd._
import org.apache.spark.sql.SparkSession

object SparkApp {
  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("test")
      .getOrCreate()
    val aero = AeroContext(sc.sparkContext).aeroSInput("192.168.142.162:3000",
      "select column1,column1,intColumn1 from test.one_million where intColumn1 between -10000000 and 10000000", sc.sqlContext ,6)
    aero.createOrReplaceTempView("aero")
    sc.sqlContext.sql("select avg(intColumn1) from aero where intColumn1 < 0").collect
  }
}
```
- java版本

```java
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import com.psbc.spark.aerospike.rdd.package$;

public class SparkApp {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("aerospike-spark");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);
        Dataset<Row> rowDataset = package$.MODULE$.AeroContext(sparkSession.sparkContext())
                .aeroSInput("localhost:3000", "select * from database.risk_mgt_variables", sqlContext, 5);
        rowDataset.show();
        rowDataset.printSchema();
        sparkSession.close;
    }
}

```
