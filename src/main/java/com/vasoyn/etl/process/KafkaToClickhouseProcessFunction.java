package com.vasoyn.etl.process;

import com.vasoyn.config.GlobalConfig;
import com.vasoyn.handle.TopicHandleProcessor;
import com.vasoyn.utils.CustomizeHandleUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author: Zhi Liu
 * Date: 2024/1/9 10:21
 * Contact: liuzhi0531@gmail.com
 */
public class KafkaToClickhouseProcessFunction  extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, List<String>>> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToClickhouseProcessFunction.class);
    // 存储获取的topic数据
    private ListState<String> topicState;
    // 定义一个 MapState存储超时时间
    private MapState<String, Long> timerState;
    // 定义计数器
    private ValueState<Long> countState;

    private Integer BATCH_SIZE = GlobalConfig.batchSize;

    private Map<String,String> filter = GlobalConfig.filter;

    private Map<String,String> add = GlobalConfig.add;

    private Map<String,String> split = GlobalConfig.split;

    private Map<String,String> customize = GlobalConfig.customizeConfig;



    // 1. open 完成初始化
    @Override
    public void open(Configuration parameters)  {
        timerState = getRuntimeContext().getMapState(new MapStateDescriptor<>("timerState", Types.STRING, Types.LONG));
        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("myListState", String.class);
        topicState = getRuntimeContext().getListState(listStateDescriptor);
        ValueStateDescriptor<Long> countStateDescriptor = new ValueStateDescriptor<>("countState", Long.class);
        countState = getRuntimeContext().getState(countStateDescriptor);
    }

    // 2. processElement进行入库操作,注册ProcessingTimeTimer定时器,完成不足批处理数量的数据入库
    @Override
    public void processElement(Tuple2<String, String> value, Context context, Collector<Tuple2<String,List<String>>> collector) throws Exception {
        String topic = value.f0;
        String content = value.f1;

        /**
         * 过滤首行，或者剔除错误数据或者添加时间戳字段
         */
        if(customize.containsKey("customize_"+topic)){
            content = CustomizeHandleUtil.getOtherJar(customize.get("customize_jar"),topic,content);
        }
        content = TopicHandleProcessor.processContentBasedOnTopic(topic,content,filter,add,split);
        if( content == null ) {
            return;
        }

        topicState.add(content);

        // 计数器部分
        Long count = countState.value();
        if (count == null) {
            count = 0L;
        }

        // 每次处理一个元素，计数器加一
        count++;

        // 将更新后的计数值保存回状态
        countState.update(count);

        if(count == 1l){
            // 注册定时器
            registerTimer(context);
        }

        // 达到批处理大小，执行数据库插入操作
        if (count %  BATCH_SIZE == 0) {
            // 执行数据库插入操作
            collector.collect(new Tuple2<>(topic,ListStateToList()));
            // 清空topicState状态
            topicState.clear();
            // 清除定时器
            deleteTimer(context);
            // 清空计数
            countState.update(0L);
        }
    }
    // 3. 流在设置时间无新流进入,定时器触发剩余数据入库操作
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String,List<String>>> out) throws Exception {
        logger.info("触发定时器: "+ LocalDateTime.now());
        String topic = ctx.getCurrentKey();
        List<String> list = ListStateToList();
        if (!list.isEmpty()) {
            // 剩余数据批量插入
            out.collect(new Tuple2<>(topic,list));
            // 清空临时存储
            topicState.clear();
        }
        // 清除定时器
        deleteTimer(ctx);
        // 清空计数
        countState.update(0L);
    }

    // ListState to List
    public List<String> ListStateToList() throws Exception {
        List<String> topicList = new ArrayList<>();
        for (String topicValue : topicState.get()) {
            topicList.add(topicValue);
        }
        return topicList;
    }

    //注册定时器
    public void registerTimer(Context context) throws Exception {
        long timerTimestamp = System.currentTimeMillis() + 10000;
        timerState.put("timer", timerTimestamp);
        context.timerService().registerProcessingTimeTimer(timerTimestamp);
    }
    //删除定时器
    public void deleteTimer(Context context) throws Exception {
        long previousTimerTimestamp = timerState.get("timer");
        context.timerService().deleteProcessingTimeTimer(previousTimerTimestamp);
    }
}
