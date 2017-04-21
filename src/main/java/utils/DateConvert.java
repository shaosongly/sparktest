package utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by shaosong on 2017/4/20.
 */
public class DateConvert {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static String getCurrentDate() {
        Date now = new Date();
        return sdf.format(now);
    }
}
