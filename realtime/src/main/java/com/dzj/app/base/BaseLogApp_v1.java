package com.dzj.app.base;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.dzj.bean.LogBean;
import com.dzj.bean.LogVideoBean;
import com.dzj.utils.MyKafkaUtil;
import com.dzj.utils.MysqlUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.table.api.Expressions.$;

// 过滤脏数据，并按照种类分流
public class BaseLogApp_v1 {
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

        // TODO 2. 读取日志数据主流
        String topic = "topic_log_kjm";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        //TODO 3.过滤掉非JSON格式的数据&将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    //会过滤null，但不会过滤{}
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });

//        jsonObjDS.print("jsonObjDS>>>>>>>");

        //TODO 4. 使用侧输出流进行分流处理，按照event_code过滤，分流
        OutputTag<String> caseTag = new OutputTag<String>("case") {
        };
        OutputTag<LogBean> videoTag = new OutputTag<LogBean>("video") {
        };

        OutputTag<String> otherTag = new OutputTag<String>("other") {
        };

        SingleOutputStreamOperator<LogBean> allDS = jsonObjDS.process(new ProcessFunction<JSONObject, LogBean>() {

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, LogBean>.Context ctx, Collector<LogBean> out) throws Exception {
                //获取类型
                String event = value.getString("event");

                if (value.getJSONObject("Item").getString("video_id") != null && ("001".equals(event) || "002".equals(event) || "003".equals(event))) {
                    ctx.output(videoTag, new LogVideoBean(
                            value.getString("deviceId"),
                            value.getString("userCode"),
                            value.getString("event"),
                            value.getString("ts"),
                            value.getJSONObject("Item").getString("video_id")
                    ));
                } else {
                    ctx.output(otherTag, value.toJSONString());
                }

            }
        });

        //TODO 5.打印侧输出流
        allDS.getSideOutput(otherTag).print("other>>>>>>");

        SideOutputDataStream<LogBean> videoDS = allDS.getSideOutput(videoTag);
        SingleOutputStreamOperator<LogVideoBean> video = videoDS.map(data -> (LogVideoBean) data);

        //注册doctor_info维度表
        tableEnv.executeSql(MysqlUtil.getDoctorInfoLookUpDDL());

        //注册video_info流
        tableEnv.executeSql(MysqlUtil.getVideoInfoLookUpDDL());

        //TODO 6.将流转换为表
        Table video_table = tableEnv.fromDataStream(video, $("deviceId"), $("userCode"), $("eventCode"), $("ts"), $("videoId"), $("pt").proctime());
//                .execute().print();

        tableEnv.createTemporaryView("video_table", video_table);
//        tableEnv.toDataStream(video_table).print("result>>>>>>>>>>");


        Table videRresultTable = tableEnv.sqlQuery(
                     "SELECT " +
                        "    vt.deviceId, " +
                        "    vt.userCode, " +
                        "    vt.eventCode, " +
                        "    vt.ts, " +
                        "    vt.videoId, " +
                        "    vi.video_length as videoLength, " +
                        "    di.level " +
                        "FROM video_table AS vt " +
                        "JOIN doctor_info FOR SYSTEM_TIME AS OF vt.pt AS di " +
                        "ON vt.userCode = di.user_id "+
                        "JOIN video_info FOR SYSTEM_TIME AS OF vt.pt AS vi " +
                        "ON vt.videoId = vi.video_id "
        );

//        tableEnv.createTemporaryView("result_table", videRresultTable);


        //TODO 7.将结果表写入到kafka
        DataStream<Row> rowDS = tableEnv.toDataStream(videRresultTable);

        SingleOutputStreamOperator<String> map = rowDS.map(
                value -> {

                    System.out.println(value.toString());
                    // 转换成JSON字符串
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("deviceId", value.getField("deviceId"));
                    jsonObject.put("userCode", value.getField("userCode"));
                    jsonObject.put("eventCode", value.getField("eventCode"));
                    jsonObject.put("ts", value.getField("ts"));
                    jsonObject.put("videoId", value.getField("videoId"));
                    jsonObject.put("videoLength", value.getField("videoLength"));
                    jsonObject.put("level", value.getField("level"));

                    return jsonObject.toJSONString();
//                   return new Gson().toJson(value);
                }
        );

        map.print("result>>>>>>>>>>");

        //建立 Kafka-Connector dwd_trade_cart_add 表
//        resultTable.execute().print();

        env.execute("BaseLogApp");


    }
}
