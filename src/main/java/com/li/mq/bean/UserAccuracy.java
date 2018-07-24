package com.li.mq.bean;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.li.mq.utils.HBaseUtil;
import lombok.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
public class UserAccuracy {

    public static final String HBASE_TABLE = "userAccuracy";

    /**
     * 答题正确率
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS = "accuracyinfo";

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
     * 课后课中做题数量
     */
    private String itemNums;

    public static void putAllUser2hbase(Configuration conf, List<AccuracyBean> accuracyList) throws Exception {

        try {

            HTable _table2 = new HTable(conf, UserAccuracy.HBASE_TABLE);

            //AccuracyBean => UserCourseAccuracyBean
            List<UserAccuracy> list = new ArrayList<>();
            if (accuracyList != null) {
                for (AccuracyBean ac : accuracyList) {
                    UserAccuracy ua = new UserAccuracy();

                    ua.setUserId(ac.getUserId());
                    ua.setCorrect(ac.getCorrect());
                    ua.setError(ac.getError());
                    ua.setNotknow(ac.getNotknow());
                    ua.setItemNums(ac.getItemNums());
                    ua.setSubmitTime(ac.getSubmitTime());
                    ua.setSum(ac.getSum());
                    ua.setAverageAnswerTime(ac.getAverageAnswerTime());
                    ua.setAccuracy(ac.getAccuracy());
                    list.add(ua);
                }
            }

            String array[][] = new String[accuracyList.size()][UserAccuracy.class.getDeclaredFields().length];
            int i = 0;
            for (UserAccuracy uca : list) {

                uca = UserAccuracy.updateUserAccuracy(uca);

                String[] row = new String[]{
                        uca.getUserId().toString(),
                        uca.getCorrect(),
                        uca.getError(),
                        uca.getNotknow(),
                        uca.getSum().toString(),
                        uca.getAccuracy().toString(),
                        uca.getSubmitTime(),
                        uca.getAverageAnswerTime().toString(),
                        uca.getItemNums()
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

                put.addColumn(Bytes.toBytes(UserAccuracy.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(UserAccuracy.HBASE_TABLE_COLUMN_CORRECT),
                        Bytes.toBytes(columns[1]));

                put.addColumn(Bytes.toBytes(UserAccuracy.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(UserAccuracy.HBASE_TABLE_COLUMN_ERROR),
                        Bytes.toBytes(columns[2]));

                put.addColumn(Bytes.toBytes(UserAccuracy.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(UserAccuracy.HBASE_TABLE_COLUMN_NOTKNOW),
                        Bytes.toBytes(columns[3]));

                put.addColumn(Bytes.toBytes(UserAccuracy.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(UserAccuracy.HBASE_TABLE_COLUMN_SUM),
                        Bytes.toBytes(columns[4]));

                put.addColumn(Bytes.toBytes(UserAccuracy.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(UserAccuracy.HBASE_TABLE_COLUMN_ACCURACY),
                        Bytes.toBytes(columns[5]));

                put.addColumn(Bytes.toBytes(UserAccuracy.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(UserAccuracy.HBASE_TABLE_COLUMN_SUBMITTIME),
                        Bytes.toBytes(columns[6]));
                put.addColumn(Bytes.toBytes(UserAccuracy.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(UserAccuracy.HBASE_TABLE_COLUMN_AVERAGEANSWERTIME),
                        Bytes.toBytes(columns[7]));

                put.addColumn(Bytes.toBytes(UserAccuracy.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(UserAccuracy.HBASE_TABLE_COLUMN_ITEMNUMS),
                        Bytes.toBytes(columns[8]));

                puts.add(put);
            }


            _table2.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static UserAccuracy updateUserAccuracy(UserAccuracy newUser) throws Exception {

        String userId = newUser.getUserId().toString();
        UserAccuracy uaFromHbase = (UserAccuracy) HBaseUtil.getObject(UserAccuracy.HBASE_TABLE, userId, UserAccuracy.class);
        if (uaFromHbase == null) {
//            put2hbase(table, newAc);
            return newUser;
        }
        uaFromHbase.setUserId(Long.parseLong(userId));
        ////////////////
        String correct = uaFromHbase.getCorrect();
        String error = uaFromHbase.getError();
        String notknow = uaFromHbase.getNotknow();
        String itemNums = uaFromHbase.getItemNums();
        Long averageAnswerTime = uaFromHbase.getAverageAnswerTime();
        Long sum = uaFromHbase.getSum();

        //////////////////////////////
        String newUserCorrect = newUser.getCorrect();
        String newUserError = newUser.getError();
        String newUserNotknow = newUser.getNotknow();
        String newUserItemNums = newUser.getItemNums();
        String newUserSubmitTime = newUser.getSubmitTime();
        Long newUserAverageAnswerTime = newUser.getAverageAnswerTime();
        Long newUserSum = newUser.getSum();

        StringBuilder corbud = new StringBuilder();
        StringBuilder errbud = new StringBuilder();
        StringBuilder notknowBud = new StringBuilder();

        try {
            corbud = AccuracyBean.mergeAnwser2(correct, newUserCorrect, newUserError, newUserNotknow, corbud);
            errbud = AccuracyBean.mergeAnwser2(error, newUserError, newUserCorrect, newUserNotknow, errbud);
            notknowBud = AccuracyBean.mergeAnwser2(notknow, newUserNotknow, newUserCorrect, newUserError, notknowBud);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        long total = 0L;
        long corrsum = 0L;
        long errosum = 0L;
        long sumnot = 0L;


        if (!corbud.toString().equals("")) {
            corrsum = corbud.toString().split(",").length;
            total += corrsum;
        }
        if (!errbud.toString().equals("")) {
            total += errbud.toString().split(",").length;
        }
        if (!notknowBud.toString().equals("")) {
            total += notknowBud.toString().split(",").length;
        }


        newUser.setCorrect(corbud.toString());
        newUser.setError(errbud.toString());
        newUser.setNotknow(notknowBud.toString());

        double accuracy = new BigDecimal(corrsum).divide(new BigDecimal(total), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
        newUser.setAccuracy(accuracy);
        newUser.setSum(newUserSum + sum);
        newUser.setAverageAnswerTime(averageAnswerTime + newUserAverageAnswerTime);
        newUser.setSubmitTime(newUserSubmitTime);
        newUser.setItemNums(itemNums(itemNums, newUserItemNums));
        return newUser;
    }

    private static String itemNums(String itemNums, String newUserItemNums) {

        String afa = "";
        String mia = "";
        if (itemNums != null && !itemNums.equals("")) {
            String[] afmi = itemNums.split("\\|");
            if (afmi[0].split("=").length > 1) {
                afa = afmi[0].split("=")[1];
            }
            if (afmi[1].split("=").length > 1) {
                mia = afmi[1].split("=")[1];
            }
        }

        String afb = "";
        String mib = "";
        if (newUserItemNums != null && !newUserItemNums.equals("")) {
            String[] afmi = newUserItemNums.split("\\|");
            if (afmi[0].split("=").length > 1) {
                afb = afmi[0].split("=")[1];
            }
            if (afmi[1].split("=").length > 1) {
                mib = afmi[1].split("=")[1];
            }
        }


        List<String> af = new ArrayList<>(Arrays.asList((afa + "," + afb).split(",")));
        StringBuilder saf = new StringBuilder();
        for (String str : af) {
            if (!saf.toString().contains(str) && !str.equals("")) {
                saf.append(str).append(",");
            }
        }

        List<String> mi = new ArrayList<>(Arrays.asList((mia + "," + mib).split(",")));
        StringBuilder smi = new StringBuilder();
        for (String str : mi) {
            if (!smi.toString().contains(str) && !str.equals("")) {
                smi.append(str).append(",");
            }
        }


        String str = "afterClass=" + saf.toString() + "|middleClass=" + smi.toString() + "";

        return str;
    }
}

