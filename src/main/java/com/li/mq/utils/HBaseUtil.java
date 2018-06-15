package com.li.mq.utils;

import com.li.mq.bean.AccuracyEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtil {

    private static Configuration conf = null;

    static {

        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir", "hdfs://192.168.65.130:9000/hbase");
    }

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir", "hdfs://192.168.65.130:9000/hbase");
    }

    @Test
    public void testDrop() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable("account");
        admin.deleteTable("account");
        admin.close();
    }


    @Test
    public void testScan() throws Exception {
        HTablePool pool = new HTablePool(conf, 10);
        HTableInterface table = pool.getTable("user");
        Scan scan = new Scan(Bytes.toBytes("rk0001"), Bytes.toBytes("rk0002"));
        scan.addFamily(Bytes.toBytes("info"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            /**
             for(KeyValue kv : r.list()){
             String family = new String(kv.getFamily());
             System.out.println(family);
             String qualifier = new String(kv.getQualifier());
             System.out.println(qualifier);
             System.out.println(new String(kv.getValue()));
             }
             */
            byte[] value = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
            System.out.println(new String(value));
        }
        pool.close();
    }


    @Test
    public void testDel() throws Exception {
        HTable table = new HTable(conf, "topic_record_accuracy_analyze");
        Delete del = new Delete(Bytes.toBytes("10"));
//        del.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("pic"));
        table.delete(del);
        table.close();
    }

//    public static void main(String[] args) throws Exception {
//
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://192.168.65.130:9000/hbase");
//
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        HTableDescriptor table = new HTableDescriptor("topic_record_accuracy_analyze");
//        HColumnDescriptor columnFamily = new HColumnDescriptor("accuracy_result");
//
//        columnFamily.setMaxVersions(10);
//        table.addFamily(columnFamily);
//        admin.createTable(table);
//        admin.close();
//
//    }


    @Test
    public void testPut() throws Exception {

        HTable table = new HTable(conf, "topic_record_accuracy_analyze");


        Put put = new Put(Bytes.toBytes("10"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("correct"), Bytes.toBytes("138"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("error"), Bytes.toBytes("113"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("sum"), Bytes.toBytes("251"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("accuracy"), Bytes.toBytes("0.55"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("submitTime"), Bytes.toBytes("2018-06-13"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("evaluationAnswerTime"), Bytes.toBytes("483"));

        table.put(put);
        table.close();
    }


    @Test
    public void testGet() throws Exception {

        //HTablePool pool = new HTablePool(conf, 10);
        //HTable table = (HTable) pool.getTable("user");
        HTable table = new HTable(conf, "topic_record_accuracy_analyze");
        Get get = new Get(Bytes.toBytes("10"));
        //get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.setMaxVersions(1);
        Result result = table.get(get);
        //result.getValue(family, qualifier)
        for (KeyValue kv : result.list()) {
            String family = new String(kv.getFamily());
            String qualifier = new String(kv.getQualifier());
            System.out.println(family + ":" + qualifier + ":" + new String(kv.getValue()));
        }
        table.close();
    }


    public static void putAll2hbase(String table, List<AccuracyEntity> accuracyList) {

        try {
            HTable _table = new HTable(conf, table);

            String array[][] = new String[accuracyList.size()][AccuracyEntity.class.getDeclaredFields().length];
            int i = 0;
            for (AccuracyEntity ac : accuracyList) {

                String[] row = new String[]{
                        ac.getUserId().toString(),
                        ac.getCorrect().toString(),
                        ac.getError().toString(),
                        ac.getSum().toString(),
                        ac.getAccuracy().toString(),
                        ac.getSubmitTime(),
                        ac.getEvaluationAnswerTime().toString()

                };

                array[i] = row;
                i++;
            }


            List<Put> puts = new ArrayList<>();
            List<Delete> deletes = new ArrayList<Delete>();

            for (int j = 0; j < array.length; j++) {

                String[] columns = array[j];
                Delete delete = new Delete(Bytes.toBytes(columns[0]));
                deletes.add(delete);


                Put put = new Put(Bytes.toBytes(columns[0]));

                put.addColumn(Bytes.toBytes(AccuracyEntity.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_CORRECT),
                        Bytes.toBytes(columns[1]));

                put.addColumn(Bytes.toBytes(AccuracyEntity.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_ERROR),
                        Bytes.toBytes(columns[2]));

                put.addColumn(Bytes.toBytes(AccuracyEntity.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_SUM),
                        Bytes.toBytes(columns[3]));

                put.addColumn(Bytes.toBytes(AccuracyEntity.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_ACCURACY),
                        Bytes.toBytes(columns[4]));

                put.addColumn(Bytes.toBytes(AccuracyEntity.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_SUBMITTIME),
                        Bytes.toBytes(columns[5]));

                put.addColumn(Bytes.toBytes(AccuracyEntity.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_COREVALUATIONANSWERTIMERECT),
                        Bytes.toBytes(columns[6]));


                puts.add(put);
            }

//            _table.delete(deletes);

            _table.put(puts);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        System.out.println(AccuracyEntity.class.getDeclaredFields().length);
    }

}
