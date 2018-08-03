package com.li.mq.bean;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.li.mq.utils.HBaseUtil;
import lombok.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
public class UserCourseAccuracyBean {

    public static final String HBASE_TABLE = "courseAccuracy";

    /**
     * accuracyinfo  列族
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS = "accuracyinfo";

    /**
     * accuracy   答题解雇
     */
    public static final String HBASE_TABLE_COLUMN_CORRECT = "accuracy";

    private String course;//userId-courseware_id-courseware_type-question_source

    private String accuracy;

    public static void putAllCourse2hbase(Configuration conf, List<AccuracyBean> accuracyList) throws Exception {

        try {

            HTable _table2 = new HTable(conf, "courseAccuracy");

            //AccuracyBean => UserCourseAccuracyBean
            List<UserCourseAccuracyBean> list = new ArrayList<>();
            if (accuracyList != null) {
                for (AccuracyBean ac : accuracyList) {
                    Long userId = ac.getUserId();
                    String cca = ac.getCourseWareCorrectAnalyze();
                    String[] arr = cca.split("\\&\\&");
                    for (int i = 0; i < arr.length; i++) {
                        UserCourseAccuracyBean uca = new UserCourseAccuracyBean();
                        String[] split = arr[i].split("\\|");
                        String course = userId + "-" + split[0].split("=")[1];
                        String accuracyinfo = split[1] + "|" + split[2] + "|" + split[3] + "|" + split[4];

                        uca.setCourse(course);
                        uca.setAccuracy(accuracyinfo);

                        list.add(uca);
                    }
                }
            }

            String array[][] = new String[list.size()][UserCourseAccuracyBean.class.getDeclaredFields().length];
            int i = 0;
            for (UserCourseAccuracyBean uca : list) {


                uca = updateCourseAccuracy(uca);


                String[] row = new String[]{
                        uca.getCourse(),
                        uca.getAccuracy()
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

                put.addColumn(Bytes.toBytes("accuracyinfo"),
                        Bytes.toBytes("accuracy"),
                        Bytes.toBytes(columns[1]));


                puts.add(put);
            }

//            _table.delete(deletes);

            _table2.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static UserCourseAccuracyBean updateCourseAccuracy(UserCourseAccuracyBean newUca) throws Exception {

        String course = newUca.getCourse();
        UserCourseAccuracyBean acFromHbase = (UserCourseAccuracyBean) HBaseUtil.getObject(UserCourseAccuracyBean.HBASE_TABLE, course, UserCourseAccuracyBean.class);
        if (acFromHbase == null) {
//            put2hbase(table, newAc);
            return newUca;
        }
        acFromHbase.setCourse(course);
        ////////////////
        String accuracyinfo = acFromHbase.getAccuracy();
        String correct = accuracyinfo.split("\\|")[0].split("=")[1];
        String error = accuracyinfo.split("\\|")[1].split("=")[1];
        String notknow = accuracyinfo.split("\\|")[2].split("=")[1];
        String cannotAnswer = accuracyinfo.split("\\|")[3].split("=")[1];

        //////////////////////////////
        String newAccuracyinfo = newUca.getAccuracy();
        String newCorrect = newAccuracyinfo.split("\\|")[0].split("=")[1];
        String newError = newAccuracyinfo.split("\\|")[1].split("=")[1];
        String newNotknow = newAccuracyinfo.split("\\|")[2].split("=")[1];
        String newCannotAnswer = newAccuracyinfo.split("\\|")[3].split("=")[1];


        StringBuilder corBud = new StringBuilder();
        StringBuilder errBud = new StringBuilder();
        StringBuilder notknowBud = new StringBuilder();
        StringBuilder cannotBud = new StringBuilder();

        try {
            corBud = AccuracyBean.mergeAnwser2(correct, newCorrect, newError, newNotknow, newCannotAnswer, corBud);
            errBud = AccuracyBean.mergeAnwser2(error, newError, newCorrect, newNotknow, newCannotAnswer, errBud);
            errBud = AccuracyBean.mergeAnwser2(notknow, newNotknow, newCorrect, newError, newCannotAnswer, errBud);
            cannotBud = AccuracyBean.mergeAnwser2(cannotAnswer, newCannotAnswer, newCorrect, newError, newNotknow, errBud);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        newUca.setAccuracy("correct=" + corBud.toString() +
                "|error=" + errBud.toString() +
                "|undo=" + notknowBud.toString()
                + "|cannot=" + cannotBud.toString());

        return newUca;

    }

}
