package com.li.mq.utils;

import com.li.mq.bean.AccuracyBean;
import com.li.mq.constants.TopicRecordConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;

public class HBaseUtil {

    private static Configuration conf = null;
    private static Connection connection;

    public static final String ZK = "192.168.100.2,192.168.100.3,192.168.100.4";
    public static final String CL = "2181";
    public static final String DIR = "/hbase";

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

        Map<String, String> map = new HashMap<>();
        for (Cell c : cells) {
            byte[] qualifier = c.getQualifier();
            byte[] valueArray = c.getValueArray();

            map.put(new String(qualifier), new String(valueArray, c.getValueOffset(), c.getValueLength()));
        }
        map.put("userId", "26");

        Class<AccuracyBean> clazz = AccuracyBean.class;
        AccuracyBean ac = clazz.newInstance();
        System.out.println(map);
        Method[] declaredMethods = clazz.getDeclaredMethods();
        for (Method m : declaredMethods) {
            String mName = m.getName();
            if (mName.startsWith("set")) {

                String set = mName.replace("set", "");
                Class pType = m.getParameterTypes()[0];

                String s1 = set.substring(0, 1).toLowerCase();
                String s2 = set.substring(1, set.length());
                if (pType.getName().equals("java.lang.Long")) {

                    m.invoke(ac, Long.parseLong(map.get(s1 + s2)));
                } else if (pType.getName().equals("java.lang.String")) {

                    m.invoke(ac, map.get(s1 + s2));
                } else if (pType.getName().equals("java.lang.Double")) {

                    m.invoke(ac, Double.parseDouble(map.get(s1 + s2)));
                }
            }

        }

        System.out.println(ac);

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

    public static AccuracyBean get(String userId) throws Exception {

        HTable table = new HTable(conf, AccuracyBean.TEST_HBASE_TABLE);
        Get get = new Get(Bytes.toBytes(userId));
        get.setMaxVersions(1);
        Result result = table.get(get);

        List<Cell> cells = result.listCells();

        Map<String, String> map = new HashMap<>();
        for (Cell c : cells) {
            byte[] qualifier = c.getQualifier();
            byte[] valueArray = c.getValueArray();

            map.put(new String(qualifier), new String(valueArray, c.getValueOffset(), c.getValueLength()));
        }
        map.put("userId", userId);

        Class<AccuracyBean> clazz = AccuracyBean.class;
        AccuracyBean ac = clazz.newInstance();
        System.out.println(map);
        Method[] declaredMethods = clazz.getDeclaredMethods();
        for (Method m : declaredMethods) {
            String mName = m.getName();
            if (mName.startsWith("set")) {

                String set = mName.replace("set", "");
                Class pType = m.getParameterTypes()[0];

                String s1 = set.substring(0, 1).toLowerCase();
                String s2 = set.substring(1, set.length());
                if (pType.getName().equals("java.lang.Long")) {

                    m.invoke(ac, Long.parseLong(map.get(s1 + s2)));
                } else if (pType.getName().equals("java.lang.String")) {

                    m.invoke(ac, map.get(s1 + s2));
                } else if (pType.getName().equals("java.lang.Double")) {

                    m.invoke(ac, Double.parseDouble(map.get(s1 + s2)));
                }
            }

        }

        table.close();

        return ac;
    }

    public static void update(String table, AccuracyBean newAc) throws Exception {

        Long userId = newAc.getUserId();
        AccuracyBean acFromHbase = HBaseUtil.get(userId.toString());

        if (acFromHbase != null && newAc != null) {
            return;
        }
        ////////////////
        Long correct = acFromHbase.getCorrect();
        Long error = acFromHbase.getError();
        Long sum = acFromHbase.getSum();
        Long count = acFromHbase.getCount();
        Long averageAnswerTime = acFromHbase.getAverageAnswerTime();
        String courseCorrectAnalyze = acFromHbase.getCourseCorrectAnalyze();
        String knowledgePointCorrectAnalyze = acFromHbase.getKnowledgePointCorrectAnalyze();
        String itemNums = acFromHbase.getItemNums();
        String[] split = itemNums.split("\\|");
        //////////////////////////////
        Long newCorrect = newAc.getCorrect();
        Long newError = newAc.getError();
        Long newSum = newAc.getSum();
        Long newAverageAnswerTime = newAc.getAverageAnswerTime();
        String newCourseCorrectAnalyze = newAc.getCourseCorrectAnalyze();
        String newKnowledgePointCorrectAnalyze = newAc.getKnowledgePointCorrectAnalyze();
        String newItemNums = newAc.getItemNums();
        String[] newSplit = newItemNums.split("\\|");
        Long newCount = newAc.getCount();

        newCount += count;
        newCorrect += correct;
        newError += error;
        newSum += sum;
        newAverageAnswerTime += averageAnswerTime;
        double accuracy = new BigDecimal(newCorrect).divide(new BigDecimal(newSum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();

        newKnowledgePointCorrectAnalyze = kn(knowledgePointCorrectAnalyze, newKnowledgePointCorrectAnalyze);
        newCourseCorrectAnalyze = cw(courseCorrectAnalyze, newCourseCorrectAnalyze);

        newItemNums = "afterClass=" + (Long.parseLong(split[0].split("=")[1]) +
                Long.parseLong(newSplit[0].split("=")[1])
        ) + "|middleClass=" + (Long.parseLong(split[1].split("=")[1]) +
                Long.parseLong(newSplit[1].split("=")[1])) + "";

        newAc.setCorrect(newCorrect);
        newAc.setError(newError);
        newAc.setSum(newSum);
        newAc.setAccuracy(accuracy);
        newAc.setCourseCorrectAnalyze(newCourseCorrectAnalyze);
        newAc.setKnowledgePointCorrectAnalyze(newKnowledgePointCorrectAnalyze);
        newAc.setItemNums(newItemNums);
        newAc.setAverageAnswerTime(newAverageAnswerTime);
        newAc.setCount(newCount);

        HBaseUtil.put2hbase(table, newAc);
    }

    private static String cw(String old, String now) {

        String courseCorrectAnalyzeInfo = old;
        Map<String, String> mapMerger = new HashMap<>();
        String[] cca1 = courseCorrectAnalyzeInfo.split("\\&\\&");
        for (String s : cca1) {//大聚合

            String courseWareId = ValueUtil.parseStr2Str(s, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            if (courseWareId.startsWith("0_")) {
                continue;
            }
            mapMerger.put(courseWareId, s);
        }

        String courseCorrectAnalyzeInfoOther = now;
        Map<String, String> mapRow = new HashMap<>();
        String[] cca2 = courseCorrectAnalyzeInfoOther.split("\\&\\&");
        for (String s2 : cca2) {
            String courseWareId = ValueUtil.parseStr2Str(s2, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            if (courseWareId.startsWith("0_")) {
                continue;
            }
            mapMerger.put(courseWareId, s2);
        }


        Set<String> courseWareIdsMerge = mapMerger.keySet();
        Set<String> courseWareIdsRow = mapRow.keySet();

        Set<String> commonSet = new HashSet<>();
        Set<String> tmp2 = new HashSet<>();
        Set<String> mergeHaveSet = new HashSet<>();
        Set<String> tmp4 = new HashSet<>();
        Set<String> tmp5 = new HashSet<>();
        Set<String> rowHaveSet = new HashSet<>();

        //找出都有的

        tmp2 = courseWareIdsRow;
        commonSet = courseWareIdsRow;
        tmp2.removeAll(courseWareIdsMerge);
        commonSet.removeAll(tmp2);


        StringBuilder upda = new StringBuilder();

        for (String id : commonSet) {

            String _courseCorrectAnalyzeInfo = mapMerger.get(id);
            long courseWareId = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            long correct = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long error = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sum = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);


            String _courseCorrectAnalyzeInfoOther = mapRow.get(id);
            long correctOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long errorOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sumOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);

            correct += correctOther;
            error += errorOther;
            sum += sumOther;

            double accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();


            upda.append("courseWareId=" + courseWareId + "|" +
                    "correct=" + correct + "|" +
                    "error=" + error + "|" +
                    "sum=" + sum + "|" +
                    "accuracy=" + accuracy + "").append("&&");
        }


        //找出merger有，row没有的
        mergeHaveSet = courseWareIdsMerge;
        tmp4 = courseWareIdsRow;
        mergeHaveSet.removeAll(tmp4);
        for (String id : mergeHaveSet) {
            upda.append(mapMerger.get(id)).append("&&");

        }
        //找出row有，merger没有的
        tmp5 = courseWareIdsMerge;
        rowHaveSet = courseWareIdsRow;
        rowHaveSet.removeAll(tmp5);
        for (String id : rowHaveSet) {
            upda.append(mapRow.get(id)).append("&&");
        }


        return upda.toString();
    }

    private static String kn(String old, String now) {


        //大聚合值
        String knowledgePointAnalyzeInfo = old;
        Map<String, String> mapMerger = new HashMap<>();
        String[] cca1 = knowledgePointAnalyzeInfo.split("\\&\\&");
        for (String s : cca1) {//大聚合

            String knowledgePoint = ValueUtil.parseStr2Str(s, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            if (knowledgePoint.equals("0_0_0,0,0")) {
                continue;
            }
            mapMerger.put(knowledgePoint, s);
        }
        //本次聚合值
        String knowledgePointAnalyzeInfoOther = now;
        Map<String, String> mapRow = new HashMap<>();
        String[] cca2 = knowledgePointAnalyzeInfoOther.split("\\&\\&");
        for (String s2 : cca2) {
            String knowledgePoint = ValueUtil.parseStr2Str(s2, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            if (knowledgePoint.equals("0_0_0,0,0")) {
                continue;
            }
            mapRow.put(knowledgePoint, s2);
        }


        Set<String> knowledgePointIdsMerge = mapMerger.keySet();
        Set<String> knowledgePointIdsRow = mapRow.keySet();

        Set<String> commonSet = new HashSet<>();
        Set<String> tmp2 = new HashSet<>();
        Set<String> mergeHaveSet = new HashSet<>();
        Set<String> tmp4 = new HashSet<>();
        Set<String> tmp5 = new HashSet<>();
        Set<String> rowHaveSet = new HashSet<>();
        Set<String> tmp7 = new HashSet<>();

        //找出都有的
        tmp2.addAll(knowledgePointIdsRow);
        commonSet.addAll(knowledgePointIdsRow);
        tmp7.addAll(knowledgePointIdsMerge);
        tmp2.removeAll(tmp7);
        commonSet.removeAll(tmp2);


        StringBuilder upda = new StringBuilder();

        for (String id : commonSet) {

            String _knowledgePointAnalyzeInfo = mapMerger.get(id);
            long knowledgePoint = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            long correct = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long error = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sum = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
            long time = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_TOTALTIME);


            String _knowledgePointAnalyzeInfoOther = mapRow.get(id);
            long correctOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            long errorOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            long sumOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
            long timeOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_TOTALTIME);

            correct += correctOther;
            error += errorOther;
            sum += sumOther;
            time += timeOther;
            double accuracy = new BigDecimal(correct).divide(new BigDecimal(sum), 2, BigDecimal.ROUND_HALF_UP).doubleValue();


            upda.append("knowledgePoint=" + knowledgePoint + "|" +
                    "correct=" + correct + "|" +
                    "error=" + error + "|" +
                    "sum=" + sum + "|" +
                    "accuracy=" + accuracy + "|" +
                    "totalTime=" + time).append("&&");
        }


        //找出merger有，row没有的
        mergeHaveSet.addAll(knowledgePointIdsMerge);
        tmp4.addAll(knowledgePointIdsRow);
        mergeHaveSet.removeAll(tmp4);
        for (String id : mergeHaveSet) {
            upda.append(mapMerger.get(id)).append("&&");

        }
        //找出row有，merger没有的
        tmp5.addAll(knowledgePointIdsMerge);
        rowHaveSet.addAll(knowledgePointIdsRow);
        rowHaveSet.removeAll(tmp5);
        for (String id : rowHaveSet) {
            upda.append(mapRow.get(id)).append("&&");
        }


        return upda.toString();
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

    public static void putPartition2hbase(Configuration conf, String table, List<AccuracyBean> accuracyList) {

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
