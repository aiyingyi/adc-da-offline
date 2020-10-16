package com.adc.da.app;

import com.adc.da.util.ComUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Test3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = ComUtil.initEnvironment();
        // 设置并行度
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties shellConfig = ComUtil.loadProperties("config/shell.properties");
        Properties odsDataConfig = ComUtil.loadProperties("config/chargeMonitor.properties");


        // 创建数据源
        KeyedStream<Tuple3<String, Long, String>, String> dataStream = env.socketTextStream("hadoop32", 7777).map(new MapFunction<String, Tuple3<String, Long, String>>() {
            @Override
            public Tuple3<String, Long, String> map(String s) throws Exception {
                JSONObject obj = JSON.parseObject(s, JSONObject.class);
                return new Tuple3(obj.getString("vin"), Long.parseLong(obj.getString("msgTime")), obj.getString("chargeStatus"));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(6))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, String> ele, long l) {
                        return ele.f1;
                    }
                })
        ).keyBy(t -> t.f0);


        Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> chargePattren = Pattern.<Tuple3<String, Long, String>>begin("charge").where(new IterativeCondition<Tuple3<String, Long, String>>() {
            @Override
            public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) {
                return "1".equals(data.f2);
            }
        }).oneOrMore().consecutive().greedy().next("uncharge").where(new IterativeCondition<Tuple3<String, Long, String>>() {
            @Override
            public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) throws Exception {
                return "0".equals(data.f2);
            }
        }).times(1);


        SingleOutputStreamOperator<Tuple3<String, Long, Long>> chargeStream = CEP.pattern(dataStream, chargePattren).select(new PatternSelectFunction<Tuple3<String, Long, String>, Tuple3<String, Long, Long>>() {

            @Override
            public Tuple3<String, Long, Long> select(Map<String, List<Tuple3<String, Long, String>>> map) throws Exception {

                List<Tuple3<String, Long, String>> chargeList = map.get("charge");
                List<Tuple3<String, Long, String>> unchargeList = map.get("uncharge");

                Tuple3<String, Long, String> tuple0 = chargeList.get(0);
                Tuple3<String, Long, String> tuple1 = unchargeList.get(0);
                return new Tuple3(tuple0.f0, tuple0.f1, tuple1.f1);
            }
        });


        chargeStream.keyBy(data->data.f0).filter(new RichFilterFunction<Tuple3<String, Long, Long>>() {

            ValueState<ChargeRecordxxx> chargeState = null;

            ChargeRecordxxx value = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                chargeState = getRuntimeContext().getState(new ValueStateDescriptor("startAndEnd", ChargeRecordxxx.class));
            }

            @Override
            public boolean filter(Tuple3<String, Long, Long> data) throws Exception {
                value = chargeState.value();

                if (value == null || value.getEndTime() != data.f2.longValue()) {
                    chargeState.update(new ChargeRecordxxx(data.f0, data.f1.longValue(), data.f2.longValue()));
                    return true;
                }
                return false;
            }

            @Override
            public void close() throws Exception {
                chargeState.clear();
            }

        }).print("充电判断");

        dataStream.print("原始数据");
        env.execute("test");
    }
}

@Data
class  ChargeRecordxxx{
    private String vin;
    private long startTime;
    private long endTime;

    public ChargeRecordxxx(String vin, long startTime, long endTime) {
        this.vin = vin;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "ChargeRecordxxx{" +
                "vin='" + vin + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}