package com.dzj.app.base;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.dzj.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.table.api.Expressions.$;

public class CreditRuleApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 1.1 开启CheckPoint
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
//        ));
//
//        // TODO 1.2 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        //会自动创建目录
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh003:8020/flink/ck");
//        System.setProperty("HADOOP_USER_NAME", "dzj_bd");

        // TODO 2. 读取业务主流
        String topic = "topic_log_kjm";
        String groupId = "credit_rule_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        // TODO 3. 主流数据结构转换
        // todo 3.1 过滤掉非JSON格式的数据&将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    //会过滤null，但不会过滤{}
                    JSONObject jsonObject = JSON.parseObject(value);
                    String userCode = jsonObject.getString("userCode");
                    String deviceId = jsonObject.getString("deviceId");
                    String eventCode = jsonObject.getString("event");
                    String ts = jsonObject.getString("ts");
                    String iteamId = jsonObject.getJSONObject("Item").getString("vedio_id");
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });

        jsonObjDS.print("jsonObjDS>>>>>>>");


        // todo 3.2 将脏数据写出到 Kafka 指定主题
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        String dirtyTopic = "dirty_data";
        dirtyDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(dirtyTopic));
        dirtyDS.print("Dirty>>>>>>>>>>");

        // TODO 4. 将主流转换成表
        // 将数据流转换成表
//        Table log_table = tableEnv.fromDataStream(jsonObjDS,$("userCode"),$("pt").proctime());
        Table log_table = tableEnv.fromDataStream(jsonObjDS,$("f0"),$("pt").proctime());

//        tableEnv.createTemporaryView("log_info", log_table);
//
//        // TODO 5. 维度表
//        tableEnv.executeSql(MysqlUtil.getDoctorInfoLookUpDDL());
//
//        Table result = tableEnv.sqlQuery("SELECT li.userCode, di.level " +
//                "FROM log_info AS li " +
//                "JOIN doctor_info FOR SYSTEM_TIME AS OF li.pt AS di " +
//                "ON li.userCode = di.user_id");

        tableEnv.toDataStream(log_table).print("result>>>>>>>>>>");


        // TODO 4.关联 docdor_info表


       env.execute("CreditRuleApp");

    }
}