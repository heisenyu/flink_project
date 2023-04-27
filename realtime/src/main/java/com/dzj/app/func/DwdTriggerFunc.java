package com.dzj.app.func;

import com.alibaba.fastjson2.JSONObject;
import com.dzj.utils.DateFormatUtil;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class DwdTriggerFunc extends Trigger<JSONObject, TimeWindow> {

    private final long delay;

    public DwdTriggerFunc(Time delay_time) {
        this.delay = delay_time.toMilliseconds();
    }

    @Override
    public TriggerResult onElement(JSONObject element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
//            System.out.println("事件——》注册定时器：" + DateFormatUtil.toYmdHms(window.maxTimestamp()));
            ctx.registerEventTimeTimer(window.maxTimestamp());

            //注册一个10秒后的定时器
//            System.out.println("处理——》注册定时器：" + DateFormatUtil.toYmdHms(window.maxTimestamp() + delay));
            ctx.registerProcessingTimeTimer(window.maxTimestamp() + delay);
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
//        System.out.println("处理——》触发窗口计算，并删除事件时间定时器，当前时间为" + DateFormatUtil.toYmdHms(time));
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()) {
            ctx.deleteProcessingTimeTimer(window.maxTimestamp() + delay);
//            System.out.println("事件——》触发窗口计算，并删除处理时间定时器");
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
//        System.out.println("清除所有定时器");
        ctx.deleteEventTimeTimer(window.maxTimestamp());
//        System.out.println("清除事件时间定时器：" + DateFormatUtil.toYmdHms(window.maxTimestamp()));
        ctx.deleteProcessingTimeTimer(window.maxTimestamp() + delay);
//        System.out.println("清除处理时间定时器：" + DateFormatUtil.toYmdHms(window.maxTimestamp() + delay));
    }

    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
//        System.out.println("合并窗口");
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
//            System.out.println("合并——》注册定时器：" + DateFormatUtil.toYmdHms(windowMaxTimestamp));
            ctx.registerEventTimeTimer(windowMaxTimestamp);
            ctx.registerProcessingTimeTimer(window.maxTimestamp() + delay);
        }
    }
}
