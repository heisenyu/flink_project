package com.dzj.app.dwd;

import com.alibaba.fastjson2.JSONObject;
import com.dzj.app.func.DwdTriggerFunc;
import com.dzj.app.func.DwdVideoProcessFunc;
import com.dzj.bean.DoctorWatchBean;
import com.dzj.utils.DateFormatUtil;
import com.dzj.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import com.dzj.utils.mysql.MysqlUtil;

import java.time.Duration;

public class DwdVideoInfoApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration configuration = new Configuration();
//        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        localEnvironmentWithWebUI.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 创建带webui的本地执行环境


        // TODO 2. 读取视频日志数据主流
        String topic = "dwd_video_info_zly";
        String groupId = "dwd_video_info_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3. 将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSONObject::parseObject);

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<JSONObject> waterDS = jsonDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> {
                            // 从 JSON 对象中提取事件时间字段的值，并返回事件时间
                            return element.getLongValue("ts");
                        }));

        //TODO 5. 分组开窗设置触发器
        SingleOutputStreamOperator<DoctorWatchBean> processed = waterDS.keyBy(new KeySelector<JSONObject, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(JSONObject value) throws Exception {
                        return new Tuple2<>(value.getString("userCode"), value.getString("resourceId"));
                    }
                }).window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .trigger(new DwdTriggerFunc(Time.seconds(10)))
                .process(new ProcessWindowFunction<JSONObject, DoctorWatchBean, Tuple2<String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple2<String, String> stringStringTuple2, ProcessWindowFunction<JSONObject, DoctorWatchBean, Tuple2<String, String>, TimeWindow>.Context ctx, Iterable<JSONObject> elements, Collector<DoctorWatchBean> out) throws Exception {
                        JSONObject resultJson = new JSONObject();
                        // 提取iterator的大小
                        int count = 0;
                        for (JSONObject element : elements) {
                            count++;
                        }

                        // 计算总时长
                        long duration = (count) * 10000L;

                        //提取视频的总时长
                        long videoLength = elements.iterator().next().getLongValue("video_length");
                        int doctor_level = elements.iterator().next().getIntValue("level");
                        System.out.println("videoLength = " + videoLength);
                        // 计算播放进度，保留两位小数
                        double progress = Math.round(duration * 100.0 / videoLength) / 100.0;


                        DoctorWatchBean doctorWatchBean = new DoctorWatchBean(stringStringTuple2.f0, doctor_level, stringStringTuple2.f1, count, (int) duration, progress);
//                        resultJson.put("user_Id", stringStringTuple2.f0);
//                        resultJson.put("resource_Id", stringStringTuple2.f1);
//                        resultJson.put("doctor_level", doctor_level);
//                        resultJson.put("count", count);
//                        resultJson.put("duration", duration);
//                        resultJson.put("video_length", videoLength);
//                        resultJson.put("progress", progress);
//                        resultJson.put("window_start", DateFormatUtil.toYmdHms(ctx.window().getStart()));
//                        resultJson.put("window_end", DateFormatUtil.toYmdHms(ctx.window().getEnd()));

                        out.collect(doctorWatchBean);
                    }
                });
//        processed.print();

        Table doctorWatchTable = tableEnv.fromDataStream(processed,
                Schema.newBuilder()
                        .column("userId", DataTypes.STRING())
                        .column("doctorLevel", DataTypes.INT())
                        .column("resourceId", DataTypes.STRING())
                        .column("count", DataTypes.INT())
                        .column("duration", DataTypes.INT())
                        .column("percentage", DataTypes.DOUBLE())
                        .columnByExpression("pt", "PROCTIME()")
                        .build());
//
        tableEnv.createTemporaryView("doctor_watch", doctorWatchTable);
//
//
//        // 创建规则表
        tableEnv.executeSql(MysqlUtil.getCreditTask());

        tableEnv.executeSql(MysqlUtil.getCreditRuleConfig());
//
        // 关联规则表和医生观看记录表

         tableEnv.executeSql(
                "SELECT " +
                        "    dw.userId, " +
                        "    dw.doctorLevel, " +
                        "    dw.resourceId, " +
//                        "    dw.count, " +
                        "    dw.duration, " +
                        "    dw.percentage, " +
                        "    ct.code AS task_code, " +
                        "    ct.task_detail," +
                        "    dw.pt " +
                        "FROM doctor_watch AS dw " +
                        "JOIN credit_rule_config FOR SYSTEM_TIME AS OF dw.pt AS crc " +
                        "ON dw.doctorLevel = crc.doctor_level "+
                        "JOIN credit_task FOR SYSTEM_TIME AS OF dw.pt AS ct " +
                        "ON ct.rule_code = crc.rule_code "
        ).print();


        env.execute("DwdVideoInfoApp");


//        SingleOutputStreamOperator<String> processed = waterDS.keyBy(new KeySelector<JSONObject, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> getKey(JSONObject value) throws Exception {
//                        return new Tuple2<>(value.getString("userCode"), value.getString("resourceId"));
//                    }
//                })
////                .process(new MyKeyedProcessFunction());
//        processed.print();
    }
    // 使用keyedProcessFunction实现计数和计时器
//    public static class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple2<String, String>, JSONObject, String> {
//
//        // 定义计数状态，类型为 MapState<String, Integer>，键类型为 String，值类型为 Integer
//        private MapState<String, Integer> countState;
//
//        // 定义计时器状态，类型为 ValueState<Long>，值类型为 Long
//        private ValueState<Long> timerState;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//
//            // 初始化状态变量
//            // 获取运行时上下文并使用 MapStateDescriptor 和 ValueStateDescriptor 分别创建计数状态和计时器状态
//            countState = getRuntimeContext().getMapState(new MapStateDescriptor<>("countState", String.class, Integer.class));
//            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerState", Long.class));
//        }
//
//        @Override
//        public void processElement(JSONObject value, Context context, Collector<String> out) throws Exception {
//            // 获取当前元素的 key
//            Tuple2<String, String> key = context.getCurrentKey();
//            String userCode = key.f0;
//            String resourceId = key.f1;
//
//            // 更新计数状态
//            // 如果该 key 的计数状态已存在，则从状态中获取计数值；否则初始化为 0
//            int count = countState.contains(userCode + resourceId) ? countState.get(userCode + resourceId) : 0;
//            count++;
//            countState.put(userCode + resourceId, count);
//
//            // 注册计时器
//            // 获取当前处理时间，并加上 10 秒，得到计时器时间
//            long timer = context.timerService().currentProcessingTime() + 10000L;
//            timerState.update(timer);
//            context.timerService().registerProcessingTimeTimer(timer);
//        }
//
//        @Override
//        public void onTimer(long timestamp, OnTimerContext context, Collector<String> out) throws Exception {
//            // 获取触发计时器的 key
//            Tuple2<String, String> key = context.getCurrentKey();
//            String userCode = key.f0;
//            String resourceId = key.f1;
//
//            // 获取计数状态
//            // 如果该 key 的计数状态已存在，则从状态中获取计数值；否则初始化为 0
//            int count = countState.contains(userCode + resourceId) ? countState.get(userCode + resourceId) : 0;
//
//            // 计算结果
//            int result = count * 10;
//
//            // 输出结果
//            out.collect(String.format("Key: (userCode:\"%s\",  resourceId:\"%s\"), count: %d, result: %d", userCode, resourceId, count, result));
//
//            // 清空状态
//            countState.remove(userCode + resourceId);
//            timerState.clear();
//        }
//    }

}



