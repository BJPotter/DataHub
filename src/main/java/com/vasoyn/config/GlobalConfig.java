package com.vasoyn.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * Author: Zhi Liu
 * Date: 2024/1/9 17:02
 * Contact: liuzhi0531@gmail.com
 * Desc: 全局初始化
 */
public class GlobalConfig {
    private static final Logger logger = LoggerFactory.getLogger(GlobalConfig.class);
    public static List<String> kafkaTopic = new ArrayList<>();
    public static Map<String, String> filter = new HashMap<>();
    public static Map<String, String> add = new HashMap<>();
    public static Map<String, String> split = new HashMap<>();
    public static Map<String, String> table = new HashMap<>();
    public static Map<String, String> dataSource = new HashMap<>();
    public static Map<String, String> kafkaConfig = new HashMap<>();
    public static Map<String, String> customizeConfig = new HashMap<>();
    public static Integer batchSize;
    public static String checkpointPath;
    public static String validate;

    private static final String TOPIC_PREFIX = "topic_";
    private static final String FILTER_SUFFIX = ".filter";
    private static final String ADD_SUFFIX = ".add";
    private static final String SPLIT_SUFFIX = ".split";
    private static final String TABLE_PREFIX = "table_";
    private static final String DATASOURCE_PREFIX = "dataSource_";
    private static final String KAFKA_PREFIX = "kafka_";
    private static final String CUSTOMIZE_PREFIX = "customize_";

    public static void initialize(Map<String, String> map) {
        try {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key.startsWith(TOPIC_PREFIX)) {
                    kafkaTopic.add(value);
                } else if (key.endsWith(FILTER_SUFFIX)) {
                    filter.put(key, value);
                } else if (key.endsWith(ADD_SUFFIX)) {
                    add.put(key, value);
                } else if (key.endsWith(SPLIT_SUFFIX)) {
                    split.put(key, value);
                } else if (key.startsWith(TABLE_PREFIX)) {
                    table.put(key, value);
                } else if (key.startsWith(DATASOURCE_PREFIX)) {
                    dataSource.put(key, value);
                } else if (key.startsWith(KAFKA_PREFIX)) {
                    kafkaConfig.put(key, value);
                } else if (key.startsWith(CUSTOMIZE_PREFIX)) {
                    customizeConfig.put(key, value);
                }
                batchSize = Integer.parseInt(map.get("batchSize"));
                checkpointPath = map.get("checkpoint");
                validate = map.get("validate");
            }
            logGlobalConfig();
        } catch (Exception e) {
            logger.error("配置文件获取失败, 全局初始化失败", e);
        }
    }

    private static void logGlobalConfig() {
        logger.info("全局初始化 Start");
        logList("Topic", kafkaTopic);
        logMap("filterKey", filter);
        logMap("addKey", add);
        logMap("splitKey", split);
        logMap("tableKey", table);
        logMap("kafkaConfigKey", kafkaConfig);
        logMap("dataSourceKey", dataSource);
        logMap("customizeConfigKey", customizeConfig);
        logger.info("批处理大小 batchSize: {}", batchSize);
        logger.info("checkPoint路径 checkpointPath: {}", checkpointPath);
        logger.info("是否开启检验(默认关闭): validate : {} {}", validate, (validate.equals("0") ? "否" : "是"));
        logger.info("全局初始化 End");
    }

    private static <T> void logList(String label, List<T> list) {
        logger.info("全局初始化 {}", label);
        for (int i = 0; i < list.size(); i++) {
            logger.info("{}{} : {}", label, (i + 1), list.get(i));
        }
    }

    private static <K, V> void logMap(String label, Map<K, V> map) {
        logger.info("全局初始化 {}", label);
        for (Map.Entry<K, V> entry : map.entrySet()) {
            logger.info("{}: {}, {}Value: {}", label, entry.getKey(), label.substring(0, label.length() - 3), entry.getValue());
        }
    }
}
