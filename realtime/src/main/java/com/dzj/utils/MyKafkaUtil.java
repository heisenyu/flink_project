package com.dzj.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.dzj.common.ComConfig;

import java.util.Properties;

public class KafkaUtil {
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ComConfig.KAFKA_BOOTSTRAP_SERVERS);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        prop.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "latest");



        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    //自定义反序列化，处理null值，因为简单的序列化出现null值会报错
                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if (record != null && record.value() != null) {
                            return new String(record.value());
                        }
                        return null;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                }, prop);
        return consumer;
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(ComConfig.KAFKA_BOOTSTRAP_SERVERS,
                topic,
                new SimpleStringSchema());
    }


}