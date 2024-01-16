package com.vasoyn.config;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Author: Zhi Liu
 * Date: 2024/1/9 10:21
 * Contact: liuzhi0531@gmail.com
 * Desc: 将Kafka中的消息反序列化为Flink数据流中的元素
 */
public class KafkaMessageConfig implements KafkaDeserializationSchema<Tuple2<String, String>> {

    //nextElement 是否表示流的最后一条元素，我们要设置为 false ,因为我们需要 msg 源源不断的被消费
    @Override
    public boolean isEndOfStream(Tuple2<String, String> stringStringTuple2) {
        return false;
    }

    // 反序列化 kafka 的 record，我们直接返回一个 tuple2<kafkaTopicName,kafkaMsgValue>
    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return new Tuple2<>(record.topic(), new String(record.value(), "UTF-8"));
    }

    //告诉 Flink 我输入的数据类型, 方便 Flink 的类型推断
    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }
}
