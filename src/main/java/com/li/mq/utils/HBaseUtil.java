package com.li.mq.utils;

import com.li.mq.bean.AccuracyEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
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

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir", "hdfs://192.168.65.130:9000/hbase");

        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor table = new HTableDescriptor("topic_record_accuracy_analyze");

        HColumnDescriptor columnFamily = new HColumnDescriptor("accuracy_result");
        columnFamily.setMaxVersions(10);
        table.addFamily(columnFamily);

        HColumnDescriptor columnFamily2 = new HColumnDescriptor("courseware_correct_analyze");
        columnFamily2.setMaxVersions(10);
        table.addFamily(columnFamily2);

        HColumnDescriptor columnFamily3 = new HColumnDescriptor("knowledgePoint_correct_analyze");
        columnFamily2.setMaxVersions(10);
        table.addFamily(columnFamily3);

        HColumnDescriptor columnFamily4 = new HColumnDescriptor("other");
        columnFamily2.setMaxVersions(10);
        table.addFamily(columnFamily4);

        admin.createTable(table);
        admin.close();

    }


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
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("evaluationAnswerTime"), Bytes.toBytes("483"));

        table.put(put);
        table.close();
    }


    @Test
    public void testGet() throws Exception {
        //HTablePool pool = new HTablePool(conf, 10);
        //HTable table = (HTable) pool.getTable("user");
        HTable table = new HTable(conf, "topic_record_accuracy_analyze");
        Get get = new Get(Bytes.toBytes("8"));
        //get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.setMaxVersions(1);
        Result result = table.get(get);
        //result.getValue(family, qualifier)


        List<Cell> cells = result.listCells();
        for (Cell c : cells) {
            byte[] familyArray = c.getQualifier();
            System.out.println(new String(familyArray) + "=" + new String(c.getValue()));
        }

        table.close();
    }

    /**
     * @param clazz   转换对象class
     * @param version 查询最多多少版本
     * @param result  hbase查询结果
     * @param ismult  是否返回集合
     * @return
     */
    private void hbase2Object(Class clazz, int version, Result result, boolean ismult) {

        Field[] declaredFields = clazz.getDeclaredFields();

        List<Cell> cells = result.listCells();
        for (Cell c : cells) {
            byte[] familyArray = c.getQualifier();
            System.out.println(new String(familyArray));
        }


//        return null;
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
                        ac.getAverageAnswerTime().toString(),
                        ac.getCourseCorrectAnalyze(),
                        ac.getKnowledgePointCorrectAnalyze(),
                        ac.getCount().toString()

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
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_AVERAGEANSWERTIME),
                        Bytes.toBytes(columns[6]));

                /**
                 * 课件答题正确率
                 */
                put.addColumn(Bytes.toBytes(AccuracyEntity.HBASE_TABLE_FAMILY_COLUMNS2),
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_COURSEWARECORRECTANALYZE),
                        Bytes.toBytes(columns[7]));

                /**
                 * 知识点答题正确率
                 */
                put.addColumn(Bytes.toBytes(AccuracyEntity.HBASE_TABLE_FAMILY_COLUMNS3),
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_KNOWLEDGEPOINTCORRECTANALYZE),
                        Bytes.toBytes(columns[8]));

                /**
                 * 知识点答题正确率
                 */
                put.addColumn(Bytes.toBytes(AccuracyEntity.HBASE_TABLE_FAMILY_COLUMNS4),
                        Bytes.toBytes(AccuracyEntity.HBASE_TABLE_COLUMN_COUNT),
                        Bytes.toBytes(columns[9]));


                puts.add(put);
            }

//            _table.delete(deletes);

            _table.put(puts);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//
//        System.out.println(AccuracyEntity.class.getDeclaredFields().length);
//    }


}
