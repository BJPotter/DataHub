package com.vasoyn.etl.sink;
import com.vasoyn.config.DruidDataSourceConfig;
import com.vasoyn.config.GlobalConfig;
import com.vasoyn.utils.DynamicDataUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.util.List;
import java.util.Map;


/**
 * Author: Zhi Liu
 * Date: 2024/1/9 10:21
 * Contact: liuzhi0531@gmail.com
 */
public class KafkaToClickhouseComonSinkFunction extends RichSinkFunction<Tuple2<String,List<String>>> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToClickhouseComonSinkFunction.class);
    private Map<String,String> table = GlobalConfig.table;
    private Map<String,String> split = GlobalConfig.split;
    private Integer batchSize = GlobalConfig.batchSize;
    private String validate = GlobalConfig.validate;
    private Map<String,String> dataSourceConfig = GlobalConfig.dataSource;

    @Override
    public void invoke(Tuple2<String, List<String>> value, Context context) throws Exception {
        String topic = value.f0;
        List<String> list = value.f1;
        try (Connection connection = DruidDataSourceConfig.getConnection(dataSourceConfig)) {
            DynamicDataUtil.insertByTopic(topic, list, table.get("table_" + topic), split.get(topic + ".split"), validate, batchSize, connection);
        } catch (Exception e) {
            logger.error("Error while processing sink operation", e);
        }
    }
}
