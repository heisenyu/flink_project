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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.table.api.Expressions.$;

// 过滤脏数据，并按照种类分流
public class BaseLogApp {
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


        //TODO 3.过滤掉非JSON格式的数据&将每行数据转换为log pojo对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<LogBean> jsonObjDS = kafkaDS.process(new ProcessFunction<String, LogBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<LogBean> out) throws Exception {

                try {
                    //会过滤null，但不会过滤{}
                    JSONObject jsonObject = JSON.parseObject(value);

                    //根据 event类型 判断log类型
                    String event = jsonObject.getString("event");
                    String eventType = "";
                    if ("001".equals(event) || "002".equals(event) || "003".equals(event)){
                        eventType = "video";
                    } else{
                        eventType = "other";
                    }

                    out.collect(
                            new LogBean(
                                    jsonObject.getString("deviceId"),
                                    jsonObject.getString("userCode"),
                                    jsonObject.getString("event"),
                                    eventType,
                                    jsonObject.getString("ts"),
                                    jsonObject.getJSONObject("Item").getString("resource_id")
                            )
                    );
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });

        // TODO 3.1 打印侧输出流
        jsonObjDS.getSideOutput(dirtyTag).print("dirty>>>>");

        // TODO 4.关联公共维度表
        // TODO 4.1 将主流转换为动态表
        Table main_table = tableEnv.fromDataStream(jsonObjDS,
                Schema
                        .newBuilder()
                        .column("deviceId", DataTypes.STRING())
                        .column("userCode", DataTypes.STRING())
                        .column("eventCode", DataTypes.STRING())
                        .column("eventType", DataTypes.STRING())
                        .column("ts", DataTypes.STRING())
                        .column("resourceId", DataTypes.STRING())
                        .columnByExpression("pt","PROCTIME()")
                        .build())
                ;
        
        // TODO 4.2 将动态表转换为临时表
        tableEnv.createTemporaryView("main_table", main_table);

//        main_table.execute().print();

        // TODO 4.2 look_up join
        // todo 4.2.1 注册doctor_info维度表，关联字段为userCode
        tableEnv.executeSql(MysqlUtil.getDoctorInfoLookUpDDL());

        Table levelRresultTable = tableEnv.sqlQuery(
                "SELECT " +
                "    mt.deviceId, " +
                "    mt.userCode, " +
                "    mt.eventCode, " +
                "    mt.eventType, " +
                "    mt.ts, " +
                "    mt.resourceId, " +
                "    mt.pt," +
                "    di.level " +
                "FROM main_table AS mt " +
                "JOIN doctor_info FOR SYSTEM_TIME AS OF mt.pt AS di " +
                "ON mt.userCode = di.user_id ");

        // 将动态表转换为临时表
        tableEnv.createTemporaryView("level_table", levelRresultTable);

        // 打印
//        tableEnv.toAppendStream(levelRresultTable, Row.class).print("level>>>>");


        // TODO 5.关联其他维度表

        // 关联video_info维度表
        // todo 5.1 注册video_info维度表，关联字段为resourceId
        tableEnv.executeSql(MysqlUtil.getVideoInfoLookUpDDL());
        Table videoRresultTable = tableEnv.sqlQuery(
                "SELECT " +
                "    lt.deviceId, " +
                "    lt.userCode, " +
                "    lt.eventCode, " +
                "    lt.ts, " +
                "    lt.resourceId, " +
                "    lt.level, " +
                "    vi.video_length " +
                "FROM level_table AS lt " +
                "JOIN video_info FOR SYSTEM_TIME AS OF lt.pt AS vi " +
                "ON lt.resourceId = vi.video_id " +
                "WHERE lt.eventType = 'video' ");

        tableEnv.createTemporaryView("video_table", videoRresultTable);

        // 打印
        tableEnv.toAppendStream(videoRresultTable, Row.class).print("video>>>>");

        // todo 5.2 建立 Kafka-Connector dwd_video_info 表
        tableEnv.executeSql("" +
                "create table dwd_video_info( " +
                "    deviceId string, " +
                "    userCode string, " +
                "    eventCode string, " +
                "    ts string, " +
                "    resourceId string, " +
                "    level int, " +
                "    video_length int " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_video_info"));

        // todo 5.3 将结果表写入到kafka
        videoRresultTable.executeInsert("dwd_video_info");







//        OutputTag<String> caseTag = new OutputTag<String>("case") {
//        };
//        OutputTag<LogBean> videoTag = new OutputTag<LogBean>("video") {
//        };
//        OutputTag<String> otherTag = new OutputTag<String>("other") {
//        };
//
//
        env.execute("BaseLogApp");


    }
}
