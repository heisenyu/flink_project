package com.dzj.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.dzj.common.ComConfig;

import java.util.Properties;

public class MyKafkaUtil {
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


    /**
     * UpsertKafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 UpsertKafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + ComConfig.KAFKA_BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }


    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {

        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + ComConfig.KAFKA_BOOTSTRAP_SERVERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets')";
    }

    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getKafkaSinkDDL(String topic) {
        return "WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + ComConfig.KAFKA_BOOTSTRAP_SERVERS + "', " +
                "  'format' = 'json' " +
                ")";
    }

}