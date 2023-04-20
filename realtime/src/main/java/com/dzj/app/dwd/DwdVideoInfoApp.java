package com.dzj.app.dwd;

import com.alibaba.fastjson2.JSONObject;
import com.dzj.app.func.DwdTriggerFunc;
import com.dzj.app.func.DwdVideoProcessFunc;
import com.dzj.utils.DateFormatUtil;
import com.dzj.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwdVideoInfoApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // TODO 2. 读取视频日志数据主流
        String topic = "dwd_video_info";
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

        //打印
//        waterDS.print("waterDS>>>>>");

        // TODO 5. 分组开窗设置触发器
        waterDS.keyBy(new KeySelector<JSONObject, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(JSONObject value) throws Exception {
                        return new Tuple2<>(value.getString("userCode"), value.getString("resourceId"));
                    }
                }).window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .trigger(new DwdTriggerFunc(Time.seconds(10)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, Tuple2<String, String>, TimeWindow>() {


                    @Override
                    public void process(Tuple2<String, String> stringStringTuple2, ProcessWindowFunction<JSONObject, JSONObject, Tuple2<String, String>, TimeWindow>.Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
                        JSONObject resultJson = new JSONObject();


                        // 提取iterator的大小
                        int count = 0;
                        for (JSONObject element : elements) {
                            count++;
                        }

                        // 计算总时长
                        long duration = (count-1) * 1000L;

                        //提取视频的总时长
                        long videoLength  = elements.iterator().next().getLongValue("video_length");

                        // 计算播放进度，保留两位小数
                        double progress = Math.round(duration * 100.0 / videoLength) / 100.0;


                        resultJson.put("user_id", stringStringTuple2.f0);
                        resultJson.put("video_id", stringStringTuple2.f1);
                        resultJson.put("count", count-1);
                        resultJson.put("duration", duration);
                        resultJson.put("video_length", videoLength);
                        resultJson.put("progress", progress);
                        resultJson.put("window_start", DateFormatUtil.toYmdHms(ctx.window().getStart()));
                        resultJson.put("window_end", DateFormatUtil.toYmdHms(ctx.window().getEnd()));

                        out.collect(resultJson);
                    }

                })
                .print(">>>>>");


        env.execute("DwdVideoInfoApp");
    }
}



