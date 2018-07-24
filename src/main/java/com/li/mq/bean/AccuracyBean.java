package com.li.mq.bean;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.li.mq.constants.TopicRecordConstant;
import com.li.mq.utils.HBaseUtil;
import com.li.mq.utils.ValueUtil;
import lombok.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
public class AccuracyBean {

    private static final SimpleDateFormat sdfYMD = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");

    public static final String TEST_HBASE_TABLE = "test_tr_accuracy_analyze2";
    public static final String HBASE_TABLE = "tr_accuracy_analyze";

    /**
     * 答题正确率
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS = "accuracy_result";

    public static final String HBASE_TABLE_COLUMN_CORRECT = "correct";
    public static final String HBASE_TABLE_COLUMN_ERROR = "error";
    public static final String HBASE_TABLE_COLUMN_NOTKNOW = "notknow";
    public static final String HBASE_TABLE_COLUMN_SUM = "sum";
    public static final String HBASE_TABLE_COLUMN_ACCURACY = "accuracy";
    public static final String HBASE_TABLE_COLUMN_SUBMITTIME = "submitTime";
    public static final String HBASE_TABLE_COLUMN_ITEMNUMS = "itemNums";
    //平均答题时间
    public static final String HBASE_TABLE_COLUMN_AVERAGEANSWERTIME = "averageAnswerTime";

    /**
     * 列族2 courseware_correct_analyze 统计课件正确率
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS2 = "courseware_correct_analyze";
    public static final String HBASE_TABLE_COLUMN_COURSEWARECORRECTANALYZE = "courseWareCorrectAnalyze";

    /**
     * 列族3 courseware_correct_analyze 统计课件正确率
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS3 = "knowledgePoint_correct_analyze";
    public static final String HBASE_TABLE_COLUMN_KNOWLEDGEPOINTCORRECTANALYZE = "knowledgePointCorrectAnalyze";

    /**
     * 列族4 count
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS4 = "other";
    public static final String HBASE_TABLE_COLUMN_COUNT = "count";


    /**
     *
     */
    private Long userId;

    /**
     *
     */
    private String correct;


    /**
     *
     */
    private String error;

    /**
     *
     */
    private String notknow;

    /**
     *
     */
    private Long sum;

    /**
     *
     */
    private Double accuracy;

    /**
     *
     */
    private String submitTime;

    /**
     * 平均答题时间
     * averageAnswerTime
     */
    private Long averageAnswerTime;

    /**
     * 课件答题正确率
     * courseWareId=639|correct=36|error=17|sum=53|accuracy=0.68&&
     * courseWareId=383|correct=40|error=25|sum=65|accuracy=0.62&&
     */
    private String courseWareCorrectAnalyze;

    /**
     * 知识点答题正确率
     * knowledgePoint=0|questionIds=0|correct=0|error=0|sum=0|accuracy=0
     */
    private String knowledgePointCorrectAnalyze;

    /**
     * count
     * count(*) group bu
     */
    private Long count;

    /**
     * 知识点正确率
     */
//    private List<AccuracyBean> knowledgePointCorrectAnalyze;

    /**
     * 课件正确率
     */
//    private List<AccuracyBean> courseWareCorrectAnalyze;

    /**
     * 课后课中做题数量
     */
    private String itemNums;

    //大聚合方法
    public static String cw(String old, String now) {

        Map<String, String> mapMerger = new HashMap<>();
        String[] cca1 = old.split("\\&\\&");
        for (String s : cca1) {//大聚合

            String courseWareId = ValueUtil.parseStr2Str(s, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);
            if (courseWareId.startsWith("0_")) {
                continue;
            }
            mapMerger.put(courseWareId, s);
        }

        Map<String, String> mapRow = new HashMap<>();
        String[] cca2 = now.split("\\&\\&");
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
            String courseWareId = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_COURSEWAREID);

            String correct = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            String error = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            String notknow = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_NOTKNOW);
            long sum = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);


            String _courseCorrectAnalyzeInfoOther = mapRow.get(id);
            String correctOther = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            String errorOther = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            String notknowOther = ValueUtil.parseStr2Str(_courseCorrectAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_NOTKNOW);
            long sumOther = ValueUtil.parseStr2Long(_courseCorrectAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);


            StringBuilder cor = new StringBuilder();
            StringBuilder err = new StringBuilder();
            StringBuilder notknows = new StringBuilder();

            cor = AccuracyBean.mergeAnwser(correct, correctOther, cor);
            err = AccuracyBean.mergeAnwser(error, errorOther, err);
            notknows = AccuracyBean.mergeAnwser(notknow, notknowOther, notknows);

            long total = 0L;
            long sumcorr = 0L;
            long sumerro = 0L;
            long sumnot = 0L;

            if (!cor.toString().equals("")) {
                sumcorr = cor.toString().split(",").length;
                total += sumcorr;
            }
            if (!err.toString().equals("")) {
                sumerro = err.toString().split(",").length;
                total += sumerro;
            }
            if (!notknows.toString().equals("")) {
                sumnot = notknow.toString().split(",").length;
                total += sumnot;
            }

            sum += sumOther;

            double accuracy = new BigDecimal(sumcorr).divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP).doubleValue();


            upda.append("courseWareId=" + courseWareId + "|" +
                    "correct=" + cor.toString() + "|" +
                    "error=" + err.toString() + "|" +
                    "notknow=" + notknows.toString() + "|" +
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

    public static String kn(String old, String now) {


        //大聚合值
        Map<String, String> mapMerger = new HashMap<>();
        String[] cca1 = old.split("\\&\\&");
        for (String s : cca1) {//大聚合

            String knowledgePoint = ValueUtil.parseStr2Str(s, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            if (knowledgePoint.equals("0_0_0,0,0")) {
                continue;
            }
            mapMerger.put(knowledgePoint, s);
        }
        //本次聚合值
        Map<String, String> mapRow = new HashMap<>();
        String[] cca2 = now.split("\\&\\&");
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
            String knowledgePoint = ValueUtil.parseStr2Str(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_KNOWLEDGEPOINT);
            String correct = ValueUtil.parseStr2Str(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            String error = ValueUtil.parseStr2Str(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            String notknow = ValueUtil.parseStr2Str(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_NOTKNOW);

            long sum = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
            long time = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfo, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_TOTALTIME);

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            String _knowledgePointAnalyzeInfoOther = mapRow.get(id);
            String correctOther = ValueUtil.parseStr2Str(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_CORRECT);
            String errorOther = ValueUtil.parseStr2Str(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_ERROR);
            String notknowOther = ValueUtil.parseStr2Str(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_NOTKNOW);

            long sumOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_SUM);
            long timeOther = ValueUtil.parseStr2Long(_knowledgePointAnalyzeInfoOther, TopicRecordConstant.SSTREAM_TOPIC_RECORD_UDAF_TOTALTIME);


            StringBuilder cor = new StringBuilder();
            StringBuilder err = new StringBuilder();
            StringBuilder notknows = new StringBuilder();

            try {
                cor = AccuracyBean.mergeAnwser2(correct, correctOther, errorOther, notknowOther, cor);
                err = AccuracyBean.mergeAnwser2(error, errorOther, correctOther, notknowOther, err);
                notknows = AccuracyBean.mergeAnwser2(notknow, notknowOther, correctOther, errorOther, notknows);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            long total = 0L;
            long sumcorr = 0L;

            if (!cor.toString().equals("")) {
                sumcorr = cor.toString().split(",").length;
                total += sumcorr;
            }
            if (!err.toString().equals("")) {
                total += err.toString().split(",").length;
            }
            if (!notknows.toString().equals("")) {
                total += notknows.toString().split(",").length;
            }

            sum += sumOther;

            double accuracy = new BigDecimal(sumcorr).divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP).doubleValue();

            time += timeOther;


            upda.append("knowledgePoint=" + knowledgePoint + "|" +
                    "correct=" + cor.toString() + "|" +
                    "error=" + err.toString() + "|" +
                    "notknow=" + notknows.toString() + "|" +
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

    public static StringBuilder mergeAnwser(String merge, String other, StringBuilder append) {
        List<String> corrMerge = Arrays.asList(merge.split(","));
        List<String> corrOther = Arrays.asList(other.split(","));

        List tmp1 = new ArrayList(corrMerge);
        List tmp2 = new ArrayList(corrOther);

        tmp2.removeAll(tmp1);
        tmp1.addAll(tmp2);

        for (int i = 0; i < tmp1.size(); i++) {
            Object o = tmp1.get(i);
            if (o != null && !o.equals("")) {

                if (i == tmp1.size() - 1) {
                    append.append(tmp1.get(i));
                } else {
                    append.append(tmp1.get(i)).append(",");
                }
            }
        }

        return append;
    }

    public static StringBuilder mergeAnwser2(String old, String toadd, String toremove1, String toremove2, StringBuilder append) throws ParseException {


        List<String> oldArr = new ArrayList(Arrays.asList(old.split(",")));
        List<String> toAddArr = new ArrayList(Arrays.asList(toadd.split(",")));
        List<String> toRemove1Arr = new ArrayList(Arrays.asList(toremove1.split(",")));
        List<String> toRemove2Arr = new ArrayList(Arrays.asList(toremove2.split(",")));
//
        boolean allHave = true;

        for (String str : oldArr) {
            if (!toAddArr.contains(str)) {
                allHave = false;
                break;
            }
        }
        if (allHave) {
            return new StringBuilder(toadd);
        }

        //corrOther没有，corrMerge有
        for (String str : oldArr) {
            if (!toAddArr.contains(str)) {
                if (!toRemove1Arr.contains(str) && !toRemove2Arr.contains(str)) {

                    toAddArr.add(str);
                }
            }
        }


        for (int i = 0; i < toAddArr.size(); i++) {
            Object o = toAddArr.get(i);
            if (o != null && !o.equals("")) {

                if (i == toAddArr.size() - 1) {
                    append.append(toAddArr.get(i));
                } else {
                    append.append(toAddArr.get(i)).append(",");
                }
            }
        }

        return append;
    }

    public static StringBuilder mergeAnwser(String merge, String submitTimeDate, String other, String submitTimeDateOther, StringBuilder append) throws ParseException {

        long old = 0L;
        long news = 0L;

        if (submitTimeDate != null && !submitTimeDate.equals("0000_00_00_00_00_00")) {
            old = sdfYMD.parse(submitTimeDate).getTime();
        }

        if (submitTimeDateOther != null && !submitTimeDateOther.equals("0000_00_00_00_00_00")) {
            news = sdfYMD.parse(submitTimeDateOther).getTime();
        }


        if (old > news) {
            String tmp = other;
            other = new String(merge);
            merge = new String(tmp);
        }


        List<String> corrMerge = Arrays.asList(merge.split(","));
        List<String> corrOther = Arrays.asList(other.split(","));

        List<String> tmp1 = new ArrayList(corrMerge);
        List<String> tmp2 = new ArrayList(corrOther);

        boolean allHave = true;

        for (String str : tmp1) {
            if (!tmp2.contains(str)) {
                allHave = false;
            }
        }
        if (allHave) {
            return new StringBuilder(other);
        }

        //corrOther没有，corrMerge有
        for (String str : tmp1) {
            if (!tmp2.contains(str)) {
                tmp2.add(str);
            }
        }

//        tmp2.removeAll(tmp1);
//        tmp1.addAll(tmp2);

        for (int i = 0; i < tmp2.size(); i++) {
            Object o = tmp2.get(i);
            if (o != null && !o.equals("")) {

                if (i == tmp2.size() - 1) {
                    append.append(tmp2.get(i));
                } else {
                    append.append(tmp2.get(i)).append(",");
                }
            }
        }

        return append;
    }

    public static StringBuilder[] answerAnalyze(String answer, StringBuilder append, StringBuilder replaceOne, StringBuilder replaceTwo) {

        StringBuilder[] arr = new StringBuilder[3];
        if (!append.toString().contains(answer)) {

            appStr(answer, append);
        }
        if (replaceOne.toString().contains(answer)) {

            replaceOne = replaceStr(answer, replaceOne);
        }
        if (replaceTwo.toString().contains(answer)) {

            replaceTwo = replaceStr(answer, replaceTwo);
        }

        arr[0] = append;
        arr[1] = replaceOne;
        arr[2] = replaceTwo;

        return arr;
    }

    private static void appStr(String answer, StringBuilder append) {
        if (append.toString().equals("")) {
            append.append(answer);
        } else {
            append.append(",").append(answer);
        }
    }

    private static StringBuilder replaceStr(String answer, StringBuilder replace) {
        String tpm = replace.toString();

        if (tpm.startsWith(answer + ",")) {

            tpm = tpm.replace(answer + ",", "");
        } else if (tpm.endsWith("," + answer)) {

            tpm = tpm.replace("," + answer, "");
        } else if (tpm.startsWith(answer + "")) {

            tpm = tpm.replace(answer + "", "");
        } else if (tpm.contains("," + answer + ",")) {

            tpm = tpm.replace("," + answer + ",", ",");
        }

        replace = new StringBuilder(tpm);
        return replace;
    }

    public static void putAllAccuracy2hbase(Configuration conf, String table, List<AccuracyBean> accuracyList) throws Exception {

        try {

            HTable _table = new HTable(conf, table);

            String array[][] = new String[accuracyList.size()][AccuracyBean.class.getDeclaredFields().length];
            int i = 0;
            for (AccuracyBean ac : accuracyList) {


                ac = updateAccuracy(table, ac);


                String[] row = new String[]{
                        ac.getUserId().toString(),
                        ac.getCorrect().toString(),
                        ac.getError().toString(),
                        ac.getSum().toString(),
                        ac.getAccuracy().toString(),
                        ac.getSubmitTime(),
                        ac.getAverageAnswerTime().toString(),
                        ac.getCourseWareCorrectAnalyze(),
                        ac.getKnowledgePointCorrectAnalyze(),
                        ac.getCount().toString(),
                        ac.getItemNums(),
                        ac.getNotknow().toString()

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
                put.addColumn(Bytes.toBytes(AccuracyBean.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(AccuracyBean.HBASE_TABLE_COLUMN_NOTKNOW),
                        Bytes.toBytes(columns[11]));

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

                puts.add(put);
            }

//            _table.delete(deletes);

            _table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static AccuracyBean updateAccuracy(String table, AccuracyBean newAc) throws Exception {

        Long userId = newAc.getUserId();
        AccuracyBean acFromHbase = (AccuracyBean) HBaseUtil.getObject(AccuracyBean.TEST_HBASE_TABLE, userId.toString(), AccuracyBean.class);

        if (acFromHbase == null) {
//            put2hbase(table, newAc);
            return newAc;
        }
        acFromHbase.setUserId(userId);
        ////////////////
        String correct = acFromHbase.getCorrect();
        String error = acFromHbase.getError();
        String notknow = acFromHbase.getNotknow();
        Long sum = acFromHbase.getSum();
        Long count = acFromHbase.getCount();
        Long averageAnswerTime = acFromHbase.getAverageAnswerTime();
        String courseCorrectAnalyze = acFromHbase.getCourseWareCorrectAnalyze();
        String knowledgePointCorrectAnalyze = acFromHbase.getKnowledgePointCorrectAnalyze();
        String itemNums = acFromHbase.getItemNums();
        String[] split = itemNums.split("\\|");


        //////////////////////////////
        String newCorrect = newAc.getCorrect();
        String newError = newAc.getError();
        String newNotknow = acFromHbase.getNotknow();
        Long newSum = newAc.getSum();
        Long newAverageAnswerTime = newAc.getAverageAnswerTime();
        String newCourseCorrectAnalyze = newAc.getCourseWareCorrectAnalyze();
        String newKnowledgePointCorrectAnalyze = newAc.getKnowledgePointCorrectAnalyze();
        String newItemNums = newAc.getItemNums();
        String[] newSplit = newItemNums.split("\\|");
        Long newCount = newAc.getCount();

        StringBuilder cor = new StringBuilder();
        StringBuilder err = new StringBuilder();
        StringBuilder notknows = new StringBuilder();


        cor = AccuracyBean.mergeAnwser(correct, newCorrect, cor);
        err = AccuracyBean.mergeAnwser(error, newError, err);
        notknows = AccuracyBean.mergeAnwser(notknow, newNotknow, notknows);

        long total = 0L;
        long sumcorr = 0L;

        if (!cor.toString().equals("")) {
            sumcorr = cor.toString().split(",").length;
            total += sumcorr;
        }
        if (!err.toString().equals("")) {
            total += err.toString().split(",").length;
        }
        if (!notknow.toString().equals("")) {
            total += notknow.toString().split(",").length;
        }

        newSum += sum;
        newCount += count;
        newAverageAnswerTime += averageAnswerTime;
        double accuracy = new BigDecimal(sumcorr).divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP).doubleValue();

        newKnowledgePointCorrectAnalyze = AccuracyBean.kn(knowledgePointCorrectAnalyze, newKnowledgePointCorrectAnalyze);
        newCourseCorrectAnalyze = AccuracyBean.cw(courseCorrectAnalyze, newCourseCorrectAnalyze);

        newItemNums = "afterClass=" + (Long.parseLong(split[0].split("=")[1]) +
                Long.parseLong(newSplit[0].split("=")[1])
        ) + "|middleClass=" + (Long.parseLong(split[1].split("=")[1]) +
                Long.parseLong(newSplit[1].split("=")[1])) + "";

        newAc.setCorrect(cor.toString());
        newAc.setError(err.toString());
        newAc.setNotknow(notknows.toString());
        newAc.setSum(newSum);
        newAc.setAccuracy(accuracy);
        newAc.setCourseWareCorrectAnalyze(newCourseCorrectAnalyze);
        newAc.setKnowledgePointCorrectAnalyze(newKnowledgePointCorrectAnalyze);
        newAc.setItemNums(newItemNums);
        newAc.setAverageAnswerTime(newAverageAnswerTime);
        newAc.setCount(newCount);

//        HBaseUtil.put2hbase(table, newAc);

        return newAc;
    }


}
