package com.adc.da.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.actors.threadpool.Arrays;

/**
 * 充电压差扩大模型算法监测充电满足条件的处理函数
 */
public class ChargeVolDiffExpProcessFunction extends KeyedProcessFunction<String, Tuple4<String, String, Double, String>, Tuple2<String, String[]>> {

    private int windowSize = 0;
    // 充电次数
    private ValueState<Long> chargeTimes = null;
    // 保存是否充电状态
    private ValueState<Boolean> isCharge = null;
    // 记录符合条件的10次充电信息
    private ValueState<String[]> chargeStartAndEnd = null;

    // 记录充电结束开始时间
    private ValueState<String> startTime = null;
    private ValueState<String> endTime = null;

    private ValueState<Double> startSoc = null;
    private ValueState<Double> endSoc = null;

    public ChargeVolDiffExpProcessFunction(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        chargeTimes = getRuntimeContext().getState(new ValueStateDescriptor<Long>("chargeTimes", Long.class));
        isCharge = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isCharge", Boolean.class));

        // 记录符合条件的10次充电信息
        chargeStartAndEnd = getRuntimeContext().getState(new ValueStateDescriptor("chargeStartAndEnd", String[].class));

        startTime = getRuntimeContext().getState(new ValueStateDescriptor("startTime", String.class));
        endTime = getRuntimeContext().getState(new ValueStateDescriptor("endTime", String.class));

        // 充电电量
        startSoc = getRuntimeContext().getState(new ValueStateDescriptor("startSoc", Double.class));
        endSoc = getRuntimeContext().getState(new ValueStateDescriptor("endSoc", Double.class));
    }

    // (vin,chargeStatus,soc,msgTime)
    @Override
    public void processElement(Tuple4<String, String, Double, String> value, Context ctx, Collector<Tuple2<String, String[]>> out) throws Exception {

        String vin = value.f0;
        String chargeStatus = value.f1;
        double soc = value.f2;
        String msgTime = value.f3;

        if ("1".equals(chargeStatus)) {
            // 如果开始充电
            if (isCharge.value() == null || isCharge.value() == false) {
                // 更新状态
                isCharge.update(true);
                startSoc.update(soc);
                endSoc.update(soc);
                startTime.update(msgTime);
                endTime.update(msgTime);
            } else { // 如果已经处于充电状态
                if (startTime.value().compareTo(msgTime) > 0)
                    startTime.update(msgTime);
                if (endTime.value().compareTo(msgTime) < 0)
                    endTime.update(msgTime);
                if (startSoc.value() > soc)
                    startSoc.update(soc);
                if (endSoc.value() < soc)
                    endSoc.update(soc);
            }
        } else { // 假如不处于充电模式
            if (isCharge.value() == true && startTime.value().compareTo(msgTime) < 0) { // 之前处于充电状态
                isCharge.update(false);
                // 只有满足条件的充电才会被加入计算
                if (startSoc.value() < 80 && endSoc.value() >= 80) {
                    // 充电次数加1
                    if (chargeTimes.value() == null) {
                        chargeTimes.update(1L);
                        chargeStartAndEnd.update(new String[2 * windowSize]);
                    } else {
                        chargeTimes.update(chargeTimes.value() + 1);
                    }
                    String[] arr = chargeStartAndEnd.value();
                    int index = (int) ((chargeTimes.value() % windowSize + 1) * 2 - 1);
                    // 更新新一轮的充电开始和结束时间
                    arr[index - 1] = startTime.value();
                    arr[index] = endTime.value();
                    // 更新状态
                    chargeStartAndEnd.update(arr);
                    if (chargeTimes.value() >= windowSize) {
                        String[] timeArray = chargeStartAndEnd.value();
                        // 时间数组按照值进行排序，是对充电进行排序
                        Arrays.sort(timeArray);
                        chargeStartAndEnd.update(timeArray);
                        out.collect(new Tuple2(vin, chargeStartAndEnd.value()));
                    }
                }
            }
        }
    }
}
