package com.psbc.spark.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 插入数据代码，用于测试
 *
 * @author Enoch on 2022/8/11
 */
public class AsPut {
    public static void main(String[] args) {
        NioEventLoops eventLoops = new NioEventLoops(1);
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.eventLoops = eventLoops;
        clientPolicy.timeout = 1000000;
        clientPolicy.loginTimeout = 1000000;
        clientPolicy.maxConnsPerNode = 500;
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.socketTimeout = 1000000;
        writePolicy.totalTimeout = 100000;
        writePolicy.sendKey = true;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.set(2020, Calendar.DECEMBER, 1);
        try (AerospikeClient client = new AerospikeClient(clientPolicy, "192.168.33.3", 13000)) {
            for (int i = 0; i <= 1000; i++) {
                System.out.println("正在插入第" + i + "条数据");
                Key key = new Key("test", "testset", "test" + i);
                cal.add(Calendar.MINUTE, 5);
                Bin create_time = new Bin("create_time", sdf.format(cal.getTime()));
                Bin col1 = new Bin("col1", "col1value" + i);
                Bin numcol = new Bin("numcol", i);
                client.put(writePolicy, key, create_time, col1, numcol);
            }
        }
    }
}