package com.dzj.utils;

import java.util.*;

import com.alibaba.fastjson2.JSONObject;
import com.dzj.common.ComConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class LogSimulator002 {

    private static final String TOPIC_NAME = "topic_log_kjm";

    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ComConfig.KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        //定义字典，存储videoId和time
        Map<String, Long> map = new HashMap<>();


        String deviceId = "abcd";
        String userCode = "";
        String[] userCodes = {"1001", "1002", "1003"};
        String event = "";
        String videoId = "";
        String[] videoIds = {"1001", "1002", "1003"};

        long time = System.currentTimeMillis();
        long startTime = 0;
        long endTime = 0;
        long gap = 0;

        map.put("ts", time);

        for (int i = 0; i < 500; i++) {
            int rand = random.nextInt(6);
            switch (rand) {
                case 0:
                    event = "001";
                    if (startTime == 0 || endTime != 0) {
//                        userCode = selectRandomUser(userCodes);
                        userCode = "1001";
                        videoId = selectRandomVideo(videoIds);
                        startTime = map.get("ts");
                        endTime = 0;
                        map.put(userCode + videoId, startTime);
                        map.put("ts", startTime);
                        JSONObject item = new JSONObject();
                        item.put("resource_id", videoId);
                        sendLog(producer, deviceId, userCode, event, map.get("ts"), item);
                    }
                    break;
                case 1:
                    if (startTime != 0 && !event.equals("003")) {
                        event = "003";
                        //获取map中的time，然后加上10s
                        endTime = map.get(userCode + videoId) + gap;
                        map.put(userCode + videoId, endTime);
                        map.put("ts", endTime);
                        JSONObject item = new JSONObject();
                        item.put("resource_id", videoId);
                        int cho = random.nextInt(2);
                        if (cho == 0) {
                            sendLog(producer, deviceId, userCode, event, map.get("ts"), item);
                        }
                    }
                    break;
                case 2:
                case 3:
                    if (startTime != 0 && endTime == 0) {
                        event = "002";
                        //获取map中的time，然后加上10s
                        long currentTime = map.get(userCode + videoId) + gap;
                        map.put(userCode + videoId, currentTime);
                        map.put("ts", currentTime);
                        JSONObject item = new JSONObject();
                        item.put("resource_id", videoId);
                        sendLog(producer, deviceId, userCode, event, map.get("ts"), item);
                    }
                    break;
            }
            Thread.sleep(1000);
        }

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

    public static String selectRandomVideo(String[] videoIds) {
        Random rand = new Random();
        int index = rand.nextInt(videoIds.length);
        return videoIds[index];
    }

    public static String selectRandomUser(String[] userCodes) {
        Random rand = new Random();
        int index = rand.nextInt(userCodes.length);
        return userCodes[index];
    }
}

