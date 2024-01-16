package com.vasoyn.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Author: Zhi Liu
 * Date: 2024/1/9 13:54
 * Contact: liuzhi0531@gmail.com
 * Desc:
 */
public class DruidDataSourceConfig {
    private static final Logger logger = LoggerFactory.getLogger(DruidDataSourceConfig.class);
    public static DruidDataSource dataSource;

    private static void initDataSource(Map<String, String> dataSourceConfig) {
        try {
        if (dataSource == null) {
            dataSource = new DruidDataSource();
            dataSource.setDriverClassName(dataSourceConfig.get("dataSource_driverClassName"));
            dataSource.setUrl(dataSourceConfig.get("dataSource_url"));
            dataSource.setUsername(dataSourceConfig.get("dataSource_username"));
            dataSource.setPassword(dataSourceConfig.get("dataSource_password"));
            dataSource.setTestOnBorrow(false);
            dataSource.setTestWhileIdle(true);
            dataSource.setTestOnReturn(false);
            dataSource.setValidationQuery("SELECT 1");
            dataSource.setMaxActive(10);
        }}catch (Exception e){
            logger.error("Failed to initialize the DruidDataSourceConfig", e);
        }
    }

    public static Connection getConnection(Map<String, String> dataSourceConfig) throws SQLException, ClassNotFoundException {
        if( dataSource == null ){
            initDataSource(dataSourceConfig);
        }
        if (dataSource == null) {
            throw new SQLException("Failed to initialize the data source");
        }
        return dataSource.getConnection();
    }
}
