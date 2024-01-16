package com.vasoyn.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Author: Zhi Liu
 * Date: 2024/1/9 10:21
 * Contact: liuzhi0531@gmail.com
 */
public class TimeUtil {

    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static String getDateTime() {
        return LocalDateTime.now().format(DATETIME_FORMATTER);
    }

    public static String getDate() {
        return LocalDateTime.now().format(DATE_FORMATTER);
    }


    public static boolean validateDateTimeFormat(String dateTimeStr) {
        dateFormat.setLenient(false);

        try {
            dateFormat.parse(dateTimeStr);
            return true;  // 如果成功解析，则返回 true
        } catch (ParseException e) {
            return false;  // 如果解析失败，则返回 false
        }
    }

    public static boolean validateDateTimeFormatRegex(String dateTimeStr){
        String regex = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$";
        return dateTimeStr.matches(regex);
    }

}
