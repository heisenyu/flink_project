package com.dzj.app.dim;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.dzj.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

import com.dzj.utils.DateFormatUtil;

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        String topic = "topic_db_02";
        String groupId = "dim_sink_app_01";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


//        kafkaDS.print("kafkaDS>>>>>>");

        // TODO 3. 主流数据结构转换
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

        jsonObjDS.print("jsonObjDS>>>>>>>");


        // 4.2 将脏数据写出到 Kafka 指定主题
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        String dirtyTopic = "dirty_data";
        dirtyDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(dirtyTopic));

        dirtyDS.print("Dirty>>>>>>>>>>");

        //todo 过滤出需要的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(data -> {
            // userCode 不为空 且 event 为 001 002 003 且 Item 中有 vedio_id 的数据
            String userCode = data.getString("userCode");
            String event = data.getString("event");
            JSONObject item = data.getJSONObject("Item");
            String vedio_id = item.getString("vedio_id");
            return userCode != null && (event.equals("001") || event.equals("002") || event.equals("003")) && vedio_id != null;
        });

//        filterDS.print("filterDS>>>>>>>>>>");

        // todo 设置水位线
        SingleOutputStreamOperator<JSONObject> waterDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> {
                            // 从 JSON 对象中提取事件时间字段的值，并返回事件时间
                            return element.getLongValue("ts");
                        })
        );


//        SingleOutputStreamOperator<JSONObject> waterDS = filterDS
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//                                    @Override
//                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                                        return element.getLong("ts");
//                                    }
//                                })
//                );


        // TODO 按照userCode 和 vedio_id 进行分组 创建会话窗口
//        KeyedStream<JSONObject, Tuple2<String, String>> keyedStream = waterDS
//                .keyBy(new KeySelector<JSONObject, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> getKey(JSONObject value) throws Exception {
//                        return Tuple2.of(value.getString("userCode"), value.getJSONObject("Item").getString("vedio_id"));
//                    }
//                });


//        keyedStream.print("keyedStream>>>>>>>>>>");


        // TODO 5. 开窗聚合
        SingleOutputStreamOperator<JSONObject> aggDS = waterDS
                .keyBy(new KeySelector<JSONObject, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(JSONObject value) throws Exception {
                        return Tuple2.of(value.getString("userCode"), value.getJSONObject("Item").getString("vedio_id"));
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(15)))
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new Trigger<JSONObject, TimeWindow>() {

                    //定义处理流的触发器时间延迟
                    private long delay = 15 * 1000L;

                    @Override
                    public TriggerResult onElement(JSONObject element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

                        System.out.println("最大时间：" +DateFormatUtil.toYmdHms(window.maxTimestamp()));
                        System.out.println("当前时间：" + DateFormatUtil.toYmdHms(ctx.getCurrentProcessingTime()));
                        System.out.println("水位线：" + DateFormatUtil.toYmdHms(ctx.getCurrentWatermark()));
                        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                            //日志打印水位线

                            // if the watermark is already past the window fire immediately
                            return TriggerResult.FIRE;
                        } else {
                            System.out.println("事件——》注册定时器：" + DateFormatUtil.toYmdHms(window.maxTimestamp()));
                            ctx.registerEventTimeTimer(window.maxTimestamp());

                            //注册一个10秒后的定时器
                            System.out.println("处理——》注册定时器：" + DateFormatUtil.toYmdHms(window.maxTimestamp() + delay));
                            ctx.registerProcessingTimeTimer(window.maxTimestamp() + delay);
                            return TriggerResult.CONTINUE;
                        }
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {

                        ctx.deleteEventTimeTimer(window.maxTimestamp());
                        System.out.println("处理——》触发窗口计算，并删除事件时间定时器，当前时间为" + DateFormatUtil.toYmdHms(time));
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        if (time == window.maxTimestamp()) {
                            ctx.deleteProcessingTimeTimer(window.maxTimestamp() + delay);
                            System.out.println("事件——》触发窗口计算，并删除处理时间定时器");
                            return TriggerResult.FIRE;
                        } else {
                            return TriggerResult.CONTINUE;
                        }
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("清除所有定时器");
                        ctx.deleteEventTimeTimer(window.maxTimestamp());
                        System.out.println("清除事件时间定时器：" + DateFormatUtil.toYmdHms(window.maxTimestamp()));
                        ctx.deleteProcessingTimeTimer(window.maxTimestamp() + delay);
                        System.out.println("清除处理时间定时器：" + DateFormatUtil.toYmdHms(window.maxTimestamp() + delay));
                    }

                    @Override
                    public boolean canMerge() {
                        return true;
                    }

                    @Override
                    public void onMerge(TimeWindow window, OnMergeContext ctx) {
                        System.out.println("合并窗口");
                        // only register a timer if the watermark is not yet past the end of the merged window
                        // this is in line with the logic in onElement(). If the watermark is past the end of
                        // the window onElement() will fire and setting a timer here would fire the window twice.
                        long windowMaxTimestamp = window.maxTimestamp();
                        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                            System.out.println("合并——》注册定时器：" + DateFormatUtil.toYmdHms(windowMaxTimestamp));
                            ctx.registerEventTimeTimer(windowMaxTimestamp);
                            ctx.registerProcessingTimeTimer(window.maxTimestamp() + delay);
                        }
                    }
                })
                .aggregate(
                        new AggregateFunction<JSONObject, JSONObject, JSONObject>() {
                            @Override
                            public JSONObject createAccumulator() {
                                JSONObject json = new JSONObject();
                                json.put("first_ts", 0L);
                                json.put("last_ts", 0L);
                                json.put("count", 0L);
                                return json;
                            }

                            @Override
                            public JSONObject add(JSONObject value, JSONObject accumulator) {
                                accumulator.put("userCode", value.getString("userCode"));
                                accumulator.put("vedio_id", value.getJSONObject("Item").getString("vedio_id"));

                                Long ts = value.getLong("ts");

                                // 取最小的 ts
                                if (accumulator.getLong("first_ts") == 0L) {
                                    accumulator.put("first_ts", ts);
                                } else {
                                    accumulator.put("first_ts", Math.min(accumulator.getLong("first_ts"), ts));
                                }

                                // 取最大的 ts
                                accumulator.put("last_ts", Math.max(accumulator.getLong("last_ts"), ts));

                                // 持续时间，转换成秒
                                accumulator.put("duration", (accumulator.getLong("last_ts") - accumulator.getLong("first_ts")) / 1000);

                                // 次数
                                accumulator.put("count", accumulator.getLong("count") + 1);

                                return accumulator;
                            }

                            @Override
                            public JSONObject getResult(JSONObject accumulator) {
                                return accumulator;
                            }

                            @Override
                            public JSONObject merge(JSONObject a, JSONObject b) {
                                JSONObject json = new JSONObject();
                                json.put("userCode", a.getString("userCode"));
                                json.put("vedio_id", a.getJSONObject("Item").getString("vedio_id"));
                                // 取最小的 ts
                                json.put("first_ts", Math.min(a.getLong("first_ts"), b.getLong("first_ts")));

                                // 取最大的 ts
                                json.put("last_ts", Math.max(a.getLong("last_ts"), b.getLong("last_ts")));

                                // 持续时间，转换成秒
                                json.put("duration", (json.getLong("last_ts") - json.getLong("first_ts")) / 1000);

                                // 次数
                                json.put("count", a.getLong("count") + b.getLong("count"));

                                return json;

                            }
                        }
                );


        aggDS.print("aggDS>>>>>>>>>>");


        env.execute("DimSinkApp");

    }
}
