package com.psbc.spark.aerospike;

import com.psbc.spark.aerospike.rdd.package$;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * 调用例子。
 * @author Enoch on 2022/8/11
 */
public class SparkApp {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("aerospike-spark");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);
        // 不带lut的查询
//        Dataset<Row> rowDataset = package$.MODULE$.AeroContext(sparkSession.sparkContext())
//                .aeroSInput("192.168.33.3:13000", "select * from test.testset", sqlContext, 5);
        Calendar beginTime = new GregorianCalendar(2022, Calendar.AUGUST, 22);
        Calendar endTime = new GregorianCalendar(2022, Calendar.AUGUST, 23);
        // 说明：月份从0开始，如果用数字的话要注意+1
        // select语句中，可以不带where。如果带where的话，where条件中的字段必须要有二级索引，否则会报错：AEROSPIKE_ERR_INDEX_NOT_FOUND
        // select语句中，最后不要加分号
        // partitionsPerServer指每个服务端节点生成多少个spark的partition，默认1个，需要根据实际数据量预估
        // 返回的结果集中，pk字段会放在最后
        Dataset<Row> rowDataset = package$.MODULE$.AeroContext(sparkSession.sparkContext())
                .aeroSInputWithLut("192.168.33.3:13000",
                        "select * from test.testset",
//                        "select * from test.testset where create_time='2020-12-01 23:26:40'",
                        sqlContext,
                        beginTime,
                        endTime,
                        5);
        rowDataset.show();
        /*
        +-------------------+------------+-------+
        |        create_time|        col1|     pk|
        +-------------------+------------+-------+
        |2020-12-01 23:56:40| col1value87| test87|
        |2020-12-01 23:26:40| col1value81| test81|
        |2020-12-01 23:46:40| col1value85| test85|
        |2020-12-02 01:06:40|col1value101|test101|
        +-------------------+------------+-------+
         */
        rowDataset.printSchema();
        /*
        root
         |-- create_time: string (nullable = true)
         |-- col1: string (nullable = true)
         |-- pk: string (nullable = true)
         */
        sparkSession.close();
    }
}

