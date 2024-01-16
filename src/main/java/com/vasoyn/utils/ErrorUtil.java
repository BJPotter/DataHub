package com.vasoyn.utils;

import com.vasoyn.config.DruidDataSourceConfig;
import com.vasoyn.enums.ErrorLevel;
import com.vasoyn.enums.ErrorType;
import com.vasoyn.config.GlobalConfig;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * Author: Zhi Liu
 * Date: 2024/1/9 10:21
 * Contact: liuzhi0531@gmail.com
 */
public class ErrorUtil {

    public static PreparedStatement preparedStatement;

    static {
        try {
            String sql = GlobalConfig.table.get("table_error_logs");
            preparedStatement = DruidDataSourceConfig.dataSource.getConnection().prepareStatement(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setCommonErrorParameters(String topic, ErrorType errorType, String message, String fieldName, ErrorLevel errorLevel) throws SQLException {
        preparedStatement.setString(1, UUIDUtil.getUUID());
        preparedStatement.setString(2, TimeUtil.getDateTime());
        preparedStatement.setString(3, errorType.toString());
        preparedStatement.setString(4, message);
        preparedStatement.setString(5, topic);
        preparedStatement.setString(6, fieldName);
        preparedStatement.setString(7, errorLevel.toString());
    }

    public static boolean validateErrorHandle(String topic, String fieldName, String message, ErrorType errorType, ErrorLevel errorLevel) throws SQLException {
        setCommonErrorParameters(topic, errorType, message, fieldName, errorLevel);
        return preparedStatement.execute();
    }

    public static String[] getField(String sql){
        String fieldsPart = sql.substring(sql.indexOf("(") + 1, sql.indexOf(")"));
        return fieldsPart.split(",\\s*");
    }

}
