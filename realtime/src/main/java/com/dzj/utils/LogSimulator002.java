package com.dzj.utils;

import java.util.*;

import com.alibaba.fastjson2.JSONObject;
import com.dzj.common.ComConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class LogSimulator {

    private static final String TOPIC_NAME = "topic_db";

    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ComConfig.KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        //定义字典，存储videoId和time
        Map<String, Long> map = new HashMap<>();


        String deviceId = "76433276587";
        String userCode = "4587483hfdkgjkdfh";
        String event = "001";
        String videoId = "";
        long time = 0;
        long startTime = 0;
        long endTime = 0;
        long gap = 1000;

        map.put("ts", time);

        for (int i = 0; i < 100; i++) {
            int rand = random.nextInt(6);
            switch (rand) {
                case 0:
                    event = "001";
                    if (startTime == 0 || endTime != 0) {
                        videoId = generateVideoId();
                        startTime = System.currentTimeMillis();
                        endTime = 0;
                        map.put(videoId, startTime);
                        map.put("ts", startTime);
                        JSONObject item = new JSONObject();
                        item.put("vedio_id", videoId);
                        item.put("strat_time", String.valueOf(startTime));
                        sendLog(producer, deviceId, userCode, event, map.get("ts"), item);
                    }
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                    if (startTime != 0 && endTime == 0) {
                        event = "002";
                        //获取map中的time，然后加上10s
                        long currentTime = map.get(videoId) + gap;
                        map.put(videoId, currentTime);
                        map.put("ts", currentTime);
                        JSONObject item = new JSONObject();
                        item.put("vedio_id", videoId);
                        item.put("currentTime", String.valueOf(currentTime));
                        sendLog(producer, deviceId, userCode, event, map.get("ts"), item);
                    }
                    break;
                case 5:
                    if (startTime != 0 && !event.equals("003")) {
                        event = "003";
                        //获取map中的time，然后加上10s
                        endTime = map.get(videoId) + gap;
                        map.put(videoId, endTime);
                        map.put("ts", endTime);
                        JSONObject item = new JSONObject();
                        item.put("vedio_id", videoId);
                        item.put("endTime", String.valueOf(endTime));
                        int cho = random.nextInt(2);
                        if (cho == 0) {
                            sendLog(producer, deviceId, userCode, event, map.get("ts"), item);
                        }

                    }
                    break;
            }
            Thread.sleep(1000);
        }

//        if (startTime != 0 && endTime == 0) {
//            event = "003";
//            endTime = System.currentTimeMillis();
//            sendLog(producer, deviceId, userCode, event, videoId, endTime);
//        }

        producer.close();
    }

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

