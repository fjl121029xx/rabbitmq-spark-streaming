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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
public class UserKnowledgeAccuracyBean {

    public static final String HBASE_TABLE = "knowAccuracy";

    /**
     * accuracyinfo  列族
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS = "accuracyinfo";

    /**
     * accuracy   答题解雇
     */
    public static final String HBASE_TABLE_COLUMN_CORRECT = "accuracy";


    private String knowledge;//userId-courseware_id-courseware_type-question_source

    private String accuracy;


    public static void putAllKnow2hbase(Configuration conf, List<AccuracyBean> accuracyList) throws Exception {

        try {

            HTable _table2 = new HTable(conf, UserKnowledgeAccuracyBean.HBASE_TABLE);

            //AccuracyBean => UserCourseAccuracyBean
            List<UserKnowledgeAccuracyBean> list = new ArrayList<>();

            if (accuracyList != null) {
                for (AccuracyBean ac : accuracyList) {

                    Long userId = ac.getUserId();
                    String kpc = ac.getKnowledgePointCorrectAnalyze();
                    String[] arr = kpc.split("\\&\\&");
                    for (int i = 0; i < arr.length; i++) {

                        UserKnowledgeAccuracyBean uka = new UserKnowledgeAccuracyBean();
                        String[] split = arr[i].split("\\|");
                        String course = userId + "&" + split[0].split("=")[1];
                        String accuracyinfo = split[1] + "|" + split[2] + "|" + split[3] + "|" + split[4];

                        uka.setKnowledge(course);
                        uka.setAccuracy(accuracyinfo);

                        list.add(uka);
                    }
                }
            }

            String array[][] = new String[list.size()][UserKnowledgeAccuracyBean.class.getDeclaredFields().length];
            int i = 0;
            for (UserKnowledgeAccuracyBean uka : list) {


                uka = updateCourseAccuracy(uka);


                String[] row = new String[]{
                        uka.getKnowledge(),
                        uka.getAccuracy()
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

    private static UserKnowledgeAccuracyBean updateCourseAccuracy(UserKnowledgeAccuracyBean newUka) throws Exception {

        String know = newUka.getKnowledge();
        UserKnowledgeAccuracyBean acFromHbase = (UserKnowledgeAccuracyBean) HBaseUtil.getObject(UserKnowledgeAccuracyBean.HBASE_TABLE, know, UserKnowledgeAccuracyBean.class);
        if (acFromHbase == null) {
//            put2hbase(table, newAc);
            return newUka;
        }
        acFromHbase.setKnowledge(know);
        ////////////////
        String accuracyinfo = acFromHbase.getAccuracy();
        String correct = accuracyinfo.split("\\|")[0].split("=")[1];
        String error = accuracyinfo.split("\\|")[1].split("=")[1];
        String notknow = accuracyinfo.split("\\|")[2].split("=")[1];
        String cannot = accuracyinfo.split("\\|")[3].split("=")[1];

        //////////////////////////////
        String newAccuracyinfo = newUka.getAccuracy();
        String newCorrect = newAccuracyinfo.split("\\|")[0].split("=")[1];
        String newError = newAccuracyinfo.split("\\|")[1].split("=")[1];
        String newNotknow = newAccuracyinfo.split("\\|")[2].split("=")[1];
        String newCannot = newAccuracyinfo.split("\\|")[4].split("=")[1];


        StringBuilder corBud = new StringBuilder();
        StringBuilder errBud = new StringBuilder();
        StringBuilder notknowBud = new StringBuilder();
        StringBuilder cannotBud = new StringBuilder();

        try {
            corBud = AccuracyBean.mergeAnwser2(correct, newCorrect, newError, newNotknow, newCannot, corBud);
            errBud = AccuracyBean.mergeAnwser2(error, newError, newCorrect, newNotknow, newCannot, errBud);
            notknowBud = AccuracyBean.mergeAnwser2(notknow, newNotknow, newCorrect, newError, newCannot, errBud);
            cannotBud = AccuracyBean.mergeAnwser2(cannot, newCannot, newCorrect, newError, newNotknow, cannotBud);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        newUka.setAccuracy("correct=" + corBud.toString() +
                "|error=" + errBud.toString() +
                "|cannot=" + cannotBud.toString() +
                "|undo=" + notknowBud.toString());

        return newUka;

    }
}
