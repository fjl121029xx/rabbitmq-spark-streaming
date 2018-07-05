package com.li.mq.utils;

import com.li.mq.bean.AccuracyBean;
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
    private static Connection connection;

    private static final String ZK = "192.168.100.2,192.168.100.3,192.168.100.4";
    private static final String CL = "2181";
    private static final String DIR = "/hbase";

    static {

        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HBaseUtil.ZK);
//        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
        conf.set("hbase.zookeeper.property.clientPort", HBaseUtil.CL);
        conf.set("hbase.rootdir", HBaseUtil.DIR);

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HBaseUtil.ZK);
//        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
        conf.set("hbase.zookeeper.property.clientPort", HBaseUtil.CL);
        conf.set("hbase.rootdir", HBaseUtil.DIR);

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void list() throws Exception {

        HBaseAdmin admin = new HBaseAdmin(conf);

        TableName[] tableNames = admin.listTableNames();

        for (int i = 0; i < tableNames.length; i++) {
            TableName tn = tableNames[i];
            System.out.println(new String(tn.getName()));
        }
        System.out.println(tableNames);
    }

    @Test
    public void testDrop() throws Exception {

        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable("test_tr_accuracy_analyze");
        admin.deleteTable("test_tr_accuracy_analyze");
        admin.close();
    }


    @Test
    public void testScan() throws Exception {
        HTablePool pool = new HTablePool(conf, 10);
        HTableInterface table = pool.getTable("test_tr_accuracy_analyze");
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
        }
        pool.close();
    }


    @Test
    public void testDel() throws Exception {
        HTable table = new HTable(conf, "test_tr_accuracy_analyze2");
        Delete del = new Delete(Bytes.toBytes("9"));
//        del.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("pic"));
        table.delete(del);
        table.close();
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
        conf.set("hbase.zookeeper.quorum", HBaseUtil.ZK);
        conf.set("hbase.zookeeper.property.clientPort", HBaseUtil.CL);
        conf.set("hbase.rootdir", HBaseUtil.DIR);

        HBaseAdmin admin = new HBaseAdmin(conf);

        HTableDescriptor table = new HTableDescriptor(AccuracyBean.TEST_HBASE_TABLE);

        HColumnDescriptor columnFamily = new HColumnDescriptor(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS);
        columnFamily.setMaxVersions(10);
        table.addFamily(columnFamily);

        HColumnDescriptor columnFamily2 = new HColumnDescriptor(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS2);
        columnFamily2.setMaxVersions(10);
        table.addFamily(columnFamily2);

        HColumnDescriptor columnFamily3 = new HColumnDescriptor(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS3);
        columnFamily2.setMaxVersions(10);
        table.addFamily(columnFamily3);

        HColumnDescriptor columnFamily4 = new HColumnDescriptor(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS4);
        columnFamily2.setMaxVersions(10);
        table.addFamily(columnFamily4);

        admin.createTable(table);
        admin.close();

    }


    @Test
    public void testPut() throws Exception {

        HTable table = new HTable(conf, "test_tr_accuracy_analyze2");


        Put put = new Put(Bytes.toBytes("101"));
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
        HTable table = new HTable(conf, "test_tr_accuracy_analyze2");
        Get get = new Get(Bytes.toBytes("26"));
        get.setMaxVersions(1);
        Result result = table.get(get);

        List<Cell> cells = result.listCells();
        for (Cell c : cells) {
            byte[] qualifier = c.getQualifier();
            byte[] valueArray = c.getValueArray();

            System.out.println(new String(qualifier) + ":" + new String(valueArray, c.getValueOffset(), c.getValueLength()));
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

    public static void put2hbase(String table, AccuracyBean ac) {

        try {
            HTable _table = new HTable(conf, table);

            String[] columns = new String[]{
                    ac.getUserId().toString(),
                    ac.getCorrect().toString(),
                    ac.getError().toString(),
                    ac.getSum().toString(),
                    ac.getAccuracy().toString(),
                    ac.getSubmitTime(),
                    ac.getAverageAnswerTime().toString(),
                    ac.getCourseCorrectAnalyze(),
                    ac.getKnowledgePointCorrectAnalyze(),
                    ac.getCount().toString(),
                    ac.getItemNums()};

            Put put = new Put(Bytes.toBytes(columns[0]));

            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_CORRECT),
                    Bytes.toBytes(columns[1]));

            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_ERROR),
                    Bytes.toBytes(columns[2]));

            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_SUM),
                    Bytes.toBytes(columns[3]));

            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_ACCURACY),
                    Bytes.toBytes(columns[4]));

            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_SUBMITTIME),
                    Bytes.toBytes(columns[5]));

            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_AVERAGEANSWERTIME),
                    Bytes.toBytes(columns[6]));

            /**
             * 课件答题正确率
             */
            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS2),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_COURSEWARECORRECTANALYZE),
                    Bytes.toBytes(columns[7]));

            /**
             * 知识点答题正确率
             */
            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS3),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_KNOWLEDGEPOINTCORRECTANALYZE),
                    Bytes.toBytes(columns[8]));

            /**
             * 知识点答题正确率
             */
            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS4),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_COUNT),
                    Bytes.toBytes(columns[9]));

            /**
             * 课后课中做题数量
             */
            put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                    Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_ITEMNUMS),
                    Bytes.toBytes(columns[10]));

            _table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putAll2hbase(String table, List<AccuracyBean> accuracyList) {

        try {

            HTable _table = new HTable(conf, table);

            String array[][] = new String[accuracyList.size()][AccuracyBean.class.getDeclaredFields().length];
            int i = 0;
            for (AccuracyBean ac : accuracyList) {

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

                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_CORRECT),
                        Bytes.toBytes(columns[1]));

                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_ERROR),
                        Bytes.toBytes(columns[2]));

                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_SUM),
                        Bytes.toBytes(columns[3]));

                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_ACCURACY),
                        Bytes.toBytes(columns[4]));

                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_SUBMITTIME),
                        Bytes.toBytes(columns[5]));

                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_AVERAGEANSWERTIME),
                        Bytes.toBytes(columns[6]));

                /**
                 * 课件答题正确率
                 */
                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS2),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_COURSEWARECORRECTANALYZE),
                        Bytes.toBytes(columns[7]));

                /**
                 * 知识点答题正确率
                 */
                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS3),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_KNOWLEDGEPOINTCORRECTANALYZE),
                        Bytes.toBytes(columns[8]));

                /**
                 * 知识点答题正确率
                 */
                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS4),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_COUNT),
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
//        System.out.println(AccuracyBean.class.getDeclaredFields().length);
//    }


}
