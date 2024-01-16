package com.vasoyn.etl;
import com.vasoyn.config.GlobalConfig;
import com.vasoyn.config.KafkaMessageConfig;
import com.vasoyn.etl.process.KafkaToClickhouseProcessFunction;
import com.vasoyn.etl.sink.KafkaToClickhouseComonSinkFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
/**
 * Author: Zhi Liu
 * Date: 2024/1/9 10:21
 * Contact: liuzhi0531@gmail.com
 */
public class KafkaToClickhouseJob {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToClickhouseJob.class);

    public static void main(String[] args)  {
        try {
            //1.设置执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //读取配置文件
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String dataHub = parameterTool.get("data_hub");
            InputStream inputStream = new FileInputStream(dataHub);
            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream,"UTF-8"));
            Properties properties = new Properties();
            properties.load(bf);
            ParameterTool dataHubSet = ParameterTool.fromMap((Map)properties);
            env.getConfig().setGlobalJobParameters(dataHubSet);
            GlobalConfig.initialize(env.getConfig().getGlobalJobParameters().toMap());
            // 开启checkpoint每,1000ms 开始一次 checkpoint,设置模式为精确一次
            env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
            // 确认 checkpoints 之间的时间会进行 500 ms
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
            // Checkpoint 必须在一分钟内完成，否则就会被抛弃
            env.getCheckpointConfig().setCheckpointTimeout(60000);
            // 允许两个连续的 checkpoint 错误
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
            // 同一时间只允许一个 checkpoint 进行
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            //通过配置来保留 checkpoint，这些被保留的 checkpoint 在作业失败或取消时不会被清除
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            //启用非对齐的检查点,提高检查点的频率
            env.getCheckpointConfig().enableUnalignedCheckpoints();
            //如果不设置，默认使用 HashMapStateBackend
            env.setStateBackend(new HashMapStateBackend());

            env.getCheckpointConfig().setCheckpointStorage(dataHubSet.get("checkpoint"));
            logger.info("设置执行环境" + LocalDateTime.now());
            //2.Kafka 连接配置
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("bootstrap.servers", dataHubSet.get("kafka_server"));
            kafkaProps.setProperty("group.id", dataHubSet.get("kafka_consumer_group"));

            logger.info("Kafka 连接配置" + LocalDateTime.now());
            //4.创建 FlinkKafkaConsumer
            FlinkKafkaConsumer<Tuple2<String, String>> kafkaConsumer = new FlinkKafkaConsumer<>(
                    GlobalConfig.kafkaTopic,
                    new KafkaMessageConfig(),
                    kafkaProps
            );
            //连接不到kafka时，增加三次重试的机会,时间为60秒
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.of(10, TimeUnit.SECONDS)));

            //5. 按主题分流入库
            DataStream<Tuple2<String, String>> dataStream = env.addSource(kafkaConsumer);
            dataStream.keyBy(data -> data.f0)
                    .process(new KafkaToClickhouseProcessFunction())
                    .addSink(new KafkaToClickhouseComonSinkFunction());

            //6. 执行作业
            env.execute("KafkaToClickhouseJob start run"+LocalDateTime.now());
        }catch (Exception e){
            logger.error("KafkaToClickhouseJob exit",e);
        }
    }
}
