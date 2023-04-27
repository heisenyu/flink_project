package com.dzj.app.dwd;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.dzj.app.func.DwdTriggerFunc;
import com.dzj.app.func.DwdVideoProcessFunc;
import com.dzj.bean.LogVideoBean;
import com.dzj.utils.DateFormatUtil;
import com.dzj.utils.JedisUtil;
import com.dzj.utils.MyKafkaUtil;
import com.dzj.utils.MysqlUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwdVideoInfoApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration configuration = new Configuration();
//        configuration.setString(RestOptions.BIND_PORT, "8081-8089");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 读取视频日志数据主流
        String topic = "dwd_video_info";
        String groupId = "dwd_video_info_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3. 将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSONObject::parseObject);

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<JSONObject> waterDS = jsonDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> {
                            // 从 JSON 对象中提取事件时间字段的值，并返回事件时间
                            return element.getLongValue("ts");
                        }));

        //打印
//        waterDS.print("waterDS>>>>>");

        // TODO 5. 分组开窗设置触发器
        KeyedStream<JSONObject, Tuple2<String, String>> keyedStream = waterDS.keyBy(new KeySelector<JSONObject, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(JSONObject value) throws Exception {
                return new Tuple2<>(value.getString("userCode"), value.getString("resourceId"));
            }
        });

//        keyedStream.print("keyedStream>>>>>");

        SingleOutputStreamOperator<LogVideoBean> processDS = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(11)))
//                .trigger(new DwdTriggerFunc(Time.seconds(10)))
                .process(new ProcessWindowFunction<JSONObject, LogVideoBean, Tuple2<String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple2<String, String> stringStringTuple2, ProcessWindowFunction<JSONObject, LogVideoBean, Tuple2<String, String>, TimeWindow>.Context ctx, Iterable<JSONObject> elements, Collector<LogVideoBean> out) throws Exception {

                        // 提取iterator的大小
                        int count = 0;
                        for (JSONObject element : elements) {
                            count++;
                        }

                        // 计算总时长
                        int duration = count * 1000;

                        //提取视频的总时长
                        int videoLength = elements.iterator().next().getIntValue("video_length");

                        //提取用户等级
                        int level = elements.iterator().next().getIntValue("level");


                        // 计算播放进度，保留两位小数
                        double percent = Math.round(duration * 100.0 / videoLength) / 100.0;


                        LogVideoBean videoBean = LogVideoBean.builder()
                                .userCode(stringStringTuple2.f0)
                                .level(level)
                                .videoId(stringStringTuple2.f1)
                                .logCount(count)
                                .duration(duration)
                                .videoLength(videoLength)
                                .videoPercent(percent)
                                .build();

                        out.collect(videoBean);
                    }

                });

        //打印
        processDS.print("processDS>>>>>");


        //  将流转换成表
        Table video_table = tableEnv.fromDataStream(processDS,
                Schema
                        .newBuilder()
                        .column("userCode", DataTypes.STRING())
                        .column("level", DataTypes.INT())
                        .column("videoId", DataTypes.STRING())
                        .column("logCount", DataTypes.INT())
                        .column("duration", DataTypes.INT())
                        .column("videoLength", DataTypes.INT())
                        .column("videoPercent", DataTypes.DOUBLE())
                        .columnByExpression("pt", "PROCTIME()")
                        .build());

        //将动态表转换为临时表
        tableEnv.createTemporaryView("video_table", video_table);
//        tableEnv.toAppendStream(video_table, Row.class).print("video_table>>>>");


        // TODO 6. 关联规则表
        tableEnv.executeSql(MysqlUtil.getCreditRuleConfigLookUpDDL());
        tableEnv.executeSql(MysqlUtil.getCreditTaskLookUpDDL());

//        Table ruleTable = tableEnv.sqlQuery(
//                "select " +
//                "   ct.code task_code, " +
//                "   ct.rule_code, " +
//                "   crc.doctor_level, " +
//                "   json_value(ct.task_detail, '$.watchVideoLengthOfTime')                rule_length, " +
//                "   json_value(ct.task_detail, '$.watchVideoWatchLengthOfTimePercentage') rule_percent " +
//                "from credit_task ct " +
//                "join credit_rule_config crc  " +
//                "on ct.rule_code = crc.rule_code");
//
//        tableEnv.createTemporaryView("rule_table", ruleTable);

        //TODO 关联维度表
        Table ruleResultTable = tableEnv.sqlQuery(
                "SELECT " +
                        "    vt.userCode, " +
                        "    vt.level, " +
                        "    vt.videoId, " +
                        "    vt.logCount, " +
                        "    vt.duration, " +
                        "    vt.videoLength, " +
                        "    vt.videoPercent, " +
                        "    ct.code taskCode, " +
                        "    ct.rule_code ruleCode, " +
                        "    json_value(ct.task_detail, '$.watchVideoLengthOfTime' RETURNING INTEGER)                ruleLength, " +
                        "    json_value(ct.task_detail, '$.watchVideoWatchLengthOfTimePercentage' RETURNING DOUBLE) rulePercent " +
                        "FROM video_table AS vt " +
                        "JOIN credit_rule_config FOR SYSTEM_TIME AS OF vt.pt AS crc " +
                        "ON vt.level = crc.doctor_level " +
                        "JOIN credit_task FOR SYSTEM_TIME AS OF vt.pt AS ct " +
                        "ON ct.rule_code = crc.rule_code "
        );

//        tableEnv.createTemporaryView("rule_result_table", ruleResultTable);
//        tableEnv.toAppendStream(ruleResultTable, Row.class).print("rule_result_table>>>>");

        //将表转换成流
        DataStream<LogVideoBean> ruleDS = tableEnv.toAppendStream(ruleResultTable, LogVideoBean.class);

        //打印
        ruleDS.print("ruleJoinDS>>>>");

        //查询redis中的数据，并进行处理
        SingleOutputStreamOperator<String> resultDS = ruleDS.process(


                new ProcessFunction<LogVideoBean, String>() {

                    private transient Jedis jedis;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        jedis = JedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        if (jedis != null) {
                            jedis.close();
                        }
                    }

                    @Override
                    public void processElement(LogVideoBean value, ProcessFunction<LogVideoBean, String>.Context ctx, Collector<String> out) throws Exception {
                        //获取key
                        String key = "video:" + value.getUserCode() + ":" + value.getVideoId() + ":" + value.getTaskCode();
                        System.out.println("key:" + key);
                        //是否存在当前的key
                        Boolean exists = jedis.exists(key);

                        if (exists) {
                            //获取value
                            boolean is_complete = Boolean.parseBoolean(jedis.hget(key, "complete"));

                            //判断是否已经完成
                            if (!is_complete) {

                                String valueStr = jedis.hget(key, "value");
                                //将value转换成JSONObject
                                JSONObject jsonObject = JSON.parseObject(valueStr);
                                //获取视频总时长
                                int videoLength = value.getVideoLength() + jsonObject.getIntValue("videoLength");
                                //获取视频播放进度
                                double videoPercent = value.getVideoPercent() + jsonObject.getDoubleValue("videoPercent");
                                //获取规则总时长
                                int ruleLength = jsonObject.getIntValue("ruleLength");
                                //获取规则播放进度
                                double rulePercent = jsonObject.getDoubleValue("rulePercent");

                                //判断是否完成
                                if (videoLength >= ruleLength && videoPercent >= rulePercent) {
                                    jedis.hset(key, "complete", "true");

                                    JSONObject resultJson = new JSONObject();
                                    resultJson.put("userCode", value.getUserCode());
                                    resultJson.put("taskCode", value.getTaskCode());
                                    resultJson.put("eventType", "video");
                                    resultJson.put("videoId", value.getVideoId());

                                    out.collect(resultJson.toJSONString());
                                    System.out.println("完成");
                                }
                                //将value转换成json字符串
                                JSONObject valueStr1 = new JSONObject();
                                valueStr1.put("videoLength", videoLength);
                                valueStr1.put("videoPercent", videoPercent);
                                valueStr1.put("ruleLength", ruleLength);
                                valueStr1.put("rulePercent", rulePercent);
                                jedis.hset(key, "value", valueStr1.toJSONString());

                                System.out.println("插入：" + JSON.toJSONString(valueStr1));


                            } else {
                                //已经完成，不做处理
                                System.out.println("已经完成，不做处理");
                            }

                        } else {
                            //第一次插入

                            //判断是否已经完成
                            if (value.getVideoLength() >= value.getRuleLength() && value.getVideoPercent() >= value.getRulePercent()) {
                                jedis.hset(key, "complete", "true");

                                JSONObject resultJson = new JSONObject();
                                resultJson.put("userCode", value.getUserCode());
                                resultJson.put("taskCode", value.getTaskCode());
                                resultJson.put("eventType", "video");
                                resultJson.put("videoId", value.getVideoId());

                                out.collect(resultJson.toJSONString());
                                System.out.println("第一次插入即完成");
                            } else {
                                jedis.hset(key, "complete", "false");
                            }

                            //将value转换成json字符串
                            JSONObject valueStr = new JSONObject();
                            valueStr.put("videoLength", value.getVideoLength());
                            valueStr.put("videoPercent", value.getVideoPercent());
                            valueStr.put("ruleLength", value.getRuleLength());
                            valueStr.put("rulePercent", value.getRulePercent());
                            jedis.hset(key, "value", valueStr.toJSONString());

                            System.out.println("第一次插入：" + JSON.toJSONString(valueStr));
                        }
                    }
                }
        );

        resultDS.print("result>>>>");


        //打印
//        tableEnv.toAppendStream(rule, Row.class).print("rule>>>>");

        env.execute("DwdVideoLogApp");
    }
}





