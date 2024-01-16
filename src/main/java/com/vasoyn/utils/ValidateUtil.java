package com.vasoyn.utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vasoyn.enums.ErrorLevel;
import com.vasoyn.enums.ErrorType;
import com.vasoyn.pojo.ValidatePojo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Map;

/**
 * Author: Zhi Liu
 * Date: 2024/1/9 10:21
 * Contact: liuzhi0531@gmail.com
 */
public class ValidateUtil {
    private static final Logger logger = LoggerFactory.getLogger(ValidateUtil.class);
    // 单例实例
    private static ValidateUtil validateUtil;

    // 存储字段配置
    private Map<String, Map<String, Map<String, ValidatePojo>>> fieldConfig;

    private ValidateUtil() {
        loadConfig();
    }

    // 静态工厂方法获取单例实例
    public static ValidateUtil getInstance() {
        if (validateUtil == null) {
            synchronized (ValidateUtil.class) {
                if (validateUtil == null) {
                    validateUtil = new ValidateUtil();
                }
            }
        }
        return validateUtil;
    }

    private void loadConfig() {
        try {
            InputStream inputStream = ValidateUtil.class.getResourceAsStream("/table_rules.json");
            ObjectMapper objectMapper = new ObjectMapper();
            fieldConfig = objectMapper.readValue(inputStream, new TypeReference<Map<String, Map<String, Map<String, ValidatePojo>>>>() {});
        } catch (IOException e) {
            logger.info("加载配置文件table_rules.json失败: "+e);
        }
    }


    /**
     * 只校验存在配置json文件中的数据表，存在字段中validateField为true的字段
     * @param tableName
     * @param fieldName
     * @return
     */
    public ValidatePojo getFieldValidate(String tableName, String fieldName) {
        Map<String, Map<String, ValidatePojo>> tableFields = fieldConfig.get(tableName);
        if (tableFields != null) {
            Map<String, ValidatePojo> fields  = tableFields.get("fields");
            if (fields != null) {
                return fields.get(fieldName);
            }
        }
        return null;
    }

    public static void validateProcess(String fieldValue,ValidatePojo validatePojo, String tableName,String fieldName) throws SQLException {
        /**
         * 判断该字段是否需要进行校验，无需校验则停止
         */
        if(!validatePojo.isValidateField()){
            return;
        }
        /**
         * 校验为空，下面不在校验
         */
        if(!validateNullable(fieldValue,validatePojo)){
            ErrorUtil.validateErrorHandle(tableName,fieldName, "Validation Nullable failed for field", ErrorType.NULLABLE, ErrorLevel.ERROR);
            return;
        }
        if(!validateType(fieldValue,validatePojo)){
            ErrorUtil.validateErrorHandle(tableName,fieldName, "Validation Type failed for field", ErrorType.TYPE, ErrorLevel.WARNING);

        }
        if(!validateRange(fieldValue,validatePojo)){
            ErrorUtil.validateErrorHandle(tableName,fieldName, "Validation Range failed for field", ErrorType.RANGE, ErrorLevel.WARNING);

        }
    }

    /**
     * 数据类型的校验目前只校验Integer和Double
     * @param fieldName
     * @param validatePojo
     * @return
     */
    public static boolean validateType(String fieldName,ValidatePojo validatePojo){
        String fieldType  = validatePojo.getType();
        if(fieldType.equals("")||fieldType==null){
            return true;
        }
        if (fieldType.equals("Integer")) {
            return validateInteger(fieldName);
        } else if (fieldType.equals("Double")) {
            return validateDouble(fieldName);
        }else {
            return false;
        }
    }

    /**
     * 校验是否为Integer
     * @param fieldName
     * @return
     */
    private static boolean validateInteger(String fieldName) {
        try {
            // 尝试将字段值转换为整数
            Integer.parseInt(fieldName);
            return true;  // 如果成功转换，则返回 true
        } catch (NumberFormatException e) {
            return false;  // 如果转换失败，则返回 false
        }
    }

    /**
     * 校验是否为Double
     * @param fieldName
     * @return
     */
    private static boolean validateDouble(String fieldName) {
        try {
            // 尝试将字段值转换为浮点数
            Double.parseDouble(fieldName);
            return true;  // 如果成功转换，则返回 true
        } catch (NumberFormatException e) {
            return false;  // 如果转换失败，则返回 false
        }
    }

    /**
     * 取值范围校验，目前仅支持Integer和Double
     * @param fieldName
     * @param validatePojo
     * @return
     */
    public static boolean validateRange(String fieldName, ValidatePojo validatePojo) {
        String range = validatePojo.getRange();
        if(range.equals("")|| range == null){
            return true;
        }
        String fieldType = validatePojo.getType();
        String[] parts = range.substring(1, range.length() - 1).split(",");
        try {
            if (fieldType.equals("Integer")) {
                int value = Integer.parseInt(fieldName);
                return isInRange(value, Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()));
            } else if (fieldType.equals("Double")) {
                double value = Double.parseDouble(fieldName);
                return isInRange(value, Double.parseDouble(parts[0].trim()), Double.parseDouble(parts[1].trim()));
            } else {
                // 其他类型的字段，可以根据实际情况进行处理
                return false;
            }
        } catch (NumberFormatException e) {
            // 处理转换异常
            return false;
        }
    }
    //使用泛型来处理不同类型的数值
    private static <T extends Comparable<T>> boolean isInRange(T value, T minValue, T maxValue) {
        return value.compareTo(minValue) >= 0 && value.compareTo(maxValue) <= 0;
    }
    //判断是否为空
    public static boolean validateNullable(String fieldName,ValidatePojo validatePojo){
        boolean nullable = validatePojo.isNullable();
        boolean isEmpty = (fieldName == null || fieldName.trim().isEmpty());
        return nullable ? true : !isEmpty;
    }

    public static void startValidate(String tableName,String fieldName,String value) throws SQLException {
        ValidateUtil validateUtil = ValidateUtil.getInstance();
        ValidatePojo validatePojo = validateUtil.getFieldValidate(tableName,fieldName);
        /**
         * 不在校验范围之内退出
         */
        if(validatePojo == null) return;
        validateUtil.validateProcess(value,validatePojo,tableName,fieldName);
    }



}
