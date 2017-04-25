package utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by shaosong on 2017/4/20.
 */
public class DateConvert {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String getDate(Long millils) {
        Date date;
        if (millils != null) {
            date = new Date(millils);
        } else {
            date = new Date();
        }
        return sdf.format(date);
    }
}
