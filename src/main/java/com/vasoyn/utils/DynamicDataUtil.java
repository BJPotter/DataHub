package com.vasoyn.utils;

import com.vasoyn.enums.ErrorLevel;
import com.vasoyn.enums.ErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.List;


public class DynamicDataUtil {
    private static final Logger logger = LoggerFactory.getLogger(DynamicDataUtil.class);

    //4. 按照topic进行入库插入
    public static void insertByTopic(String topic, List<String> values,String table,String split,String validate,Integer batchSize,Connection connection) throws Exception{

        // 根据 topic 获取对应的 SQL 语句
        String insertSql = table;
        String sep = split;
        // 获取不到对应的 SQL 语句抛出异常
        if (insertSql == null || sep == null) {
            throw new IllegalArgumentException("Unsupported topic: " + topic);
        }
        // 获取表字段的个数
        long fieldCount = insertSql.chars()
                .filter(ch -> ch == '?')
                .count();
        // 获取字段名称
        String[] validateField = ErrorUtil.getField(insertSql);

        //取消自动提交,需要clickhouse开启事务，暂未开启
        //connection.setAutoCommit(false);

        PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
        // 计数器
        int count = 0;

        try {
            for (String value : values) {
                String[] data = value.split(sep.replace("|", "\\|"));
                //校验
                if(data.length > fieldCount){
                    if(!validate.equals("0")){
                        ErrorUtil.validateErrorHandle(topic,"" , "数据长度大于表字段长度", ErrorType.TableNotMatch, ErrorLevel.ERROR);
                        continue;
                    }
                }
                for (int i = 0; i < fieldCount; i++) {
                    //校验
                    if (i < data.length) {
                        if(!validate.equals("0")) {
                            ValidateUtil.startValidate(topic, validateField[i], data[i]);
                        }
                        preparedStatement.setString(i + 1, data[i]);
                    } else {
                        // 如果 data 数组的长度不足以填满参数列表，则将多余的参数设置为默认值
                        preparedStatement.setNull(i + 1, Types.VARCHAR);
                    }
                }
                preparedStatement.addBatch(); // 将当前数据添加到批量插入中
                count++;
                if (count % batchSize == 0) {
                    preparedStatement.executeBatch(); // 执行批量插入操作
                    preparedStatement.clearBatch(); // 清空批量插入列表
                }
            }
            //防止遗漏再次提交
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
        }catch (Exception  e){
            logger.error(topic+" insert error",e);
        }finally {
            //关闭资源
            preparedStatement.close();
            connection.close();
        }
    }
}
