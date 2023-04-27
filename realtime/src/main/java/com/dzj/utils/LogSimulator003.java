package com.dzj.utils;

import com.alibaba.fastjson2.JSONObject;
import com.dzj.common.ComConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class LogSimulator003 {

    private static final String TOPIC_NAME = "topic_log_zly";

    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ComConfig.KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        //定义字典，存储videoId和time
        Map<String, Long> map = new HashMap<>();


        String deviceId = "abcde";
        String userCode = "1001";
        String event = "001";
        String videoId = "";
        long time = 0;
        long startTime = 0;

        map.put("ts", time);

        for (int i = 0; i < 10; i++) {
            if (i == 0) {
                event = "001";
//                videoId = generateVideoId();
                videoId = "1001";
                startTime = System.currentTimeMillis();

                JSONObject item = new JSONObject();
                item.put("vedio_id", videoId);
                item.put("resource_id", videoId);
                item.put("strat_time", String.valueOf(startTime));
                sendLog(producer, deviceId, userCode, event, startTime, item);
            } else {
                event = "002";
                long currentTime = System.currentTimeMillis();
                JSONObject item = new JSONObject();
                item.put("vedio_id", videoId);
                item.put("resource_id", videoId);
                item.put("currentTime", String.valueOf(currentTime));
                sendLog(producer, deviceId, userCode, event, currentTime, item);
            }
            Thread.sleep(9000);
        }
        producer.close();

    }

//        if (startTime != 0 && endTime == 0) {
//            event = "003";
//            endTime = System.currentTimeMillis();
//            sendLog(producer, deviceId, userCode, event, videoId, endTime);
//        }


    private static void sendLog(Producer<String, String> producer, String deviceId, String userCode, String event, long ts, JSONObject item) {
        JSONObject log = new JSONObject();
        log.put("deviceId", deviceId);
        log.put("userCode", userCode);
        log.put("event", event);
        log.put("ts", ts);
        log.put("Item", item);
        System.out.println(log);
        producer.send(new ProducerRecord<>(TOPIC_NAME, null, log.toString()));
    }

    private static String generateVideoId() {
        return String.valueOf(random.nextInt(10000));
    }
}

