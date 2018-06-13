package com.li.mq.rabbit.utils;

public class ValueUtil {

    public static Long parseStr2Long(String value, String field) {

        if (value == null && value.equals("")) {

            return 0L;
        }
        String[] fields = value.split("\\|");

        for (String fie : fields) {

            if (fie.startsWith(field)) {
                try {
                    long result = Long.parseLong(fie.split("=")[1]);
                    return result;
                } catch (Exception e) {
                    break;
                }
            }

        }
        return 0L;
    }

    public static Integer parseStr2Int(String value, String field) {

        if (value == null && value.equals("")) {

            return 0;
        }
        String[] fields = value.split("\\|");

        for (String fie : fields) {

            if (fie.startsWith(field)) {

                try {

                    Integer result = Integer.parseInt(fie.split("=")[1]);
                    return result;
                } catch (Exception e) {
                    break;
                }
            }
        }
        return 0;
    }
}
