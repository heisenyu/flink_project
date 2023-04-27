package com.dzj.utils;

import com.dzj.common.ComConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @beLongProject: IntelliJ IDEA
 * @beLongPackage: mavendemo
 * @author: Deamon
 * @createTime: 2023/03/30 14:36
 * @company: http://www.dazhuanjia.com/
 * @description: 4月份积分兑换需求用户日志写入 kafka
 */
public class MyKafkaProducer {
    public static void main(String[] args) throws IOException, InterruptedException {
        // 配置文件对象
        Properties props = new Properties();
        // kafka broker 节点列表
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ComConfig.KAFKA_BOOTSTRAP_SERVERS);

        // kv 序列化方式
        props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // 3. 创建kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("realtime/src/main/java/com/dzj/utils/user_log_ofcredit"))) {
            String str;
            while ((str = bufferedReader.readLine()) != null) {

                System.out.println(str);

                // 写入 topic
                ProducerRecord<String, String> record = new ProducerRecord<>("topic_log_kjm", null, str);

                // 将数据发送到 kafka
                producer.send(record).get();
                producer.flush();
                Thread.sleep(1000);
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {

            producer.close();

        }

    }
}

