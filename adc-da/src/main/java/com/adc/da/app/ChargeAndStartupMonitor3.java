package com.adc.da.app;

import com.adc.da.bean.OdsData;
import com.adc.da.bean.chargeAndDisChargeInfo;
import com.adc.da.util.ComUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import groovy.lang.Lazy;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 监控充电完成和行驶结束，触发计算
 */
public class ChargeAndStartupMonitor3 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = ComUtil.initEnvironment();
        // 设置并行度
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties shellConfig = ComUtil.loadProperties("config/shell.properties");
        Properties odsDataConfig = ComUtil.loadProperties("config/odsTopic.properties");
        // 创建数据源,提取水位线并设置WaterMark的延时
        //KeyedStream<OdsData, String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>(odsDataConfig.getProperty("topic"), new SimpleStringSchema(), odsDataConfig)).map(new MapFunction<String, OdsData>() {
        KeyedStream<OdsData, String> dataStream = env.socketTextStream("hadoop32", 7777).map(new MapFunction<String, OdsData>() {
            @Override
            public OdsData map(String data) {
                OdsData ods = new OdsData();
                JSONObject obj = JSON.parseObject(data, JSONObject.class);
                double soc = Double.parseDouble(obj.getString("soc").substring(0, obj.getString("soc").length() - 1));
                ods.setVin(obj.getString("vin"));
                ods.setMsgTime(obj.getLong("msgTime"));
                ods.setSpeed(obj.getDouble("speed"));
                ods.setStartupStatus(obj.getString("startupStatus"));
                ods.setGearStatus(obj.getString("gearStatus"));
                ods.setChargeStatus(obj.getString("chargeStatus"));
                ods.setSoc(soc);
                ods.setOdo(obj.getDouble("odo"));
                // 解析电压数组
                String cellVoltage = obj.getString("cellVoltage");
                double[] vols = Arrays.stream(cellVoltage.substring(1, cellVoltage.length() - 2).split(",")).mapToDouble(vol -> Double.parseDouble(vol)).toArray();
                ods.setCellVoltage(vols);
                return ods;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OdsData>forBoundedOutOfOrderness(Duration.ofSeconds(6))
                .withTimestampAssigner(new SerializableTimestampAssigner<OdsData>() {
                    @Override
                    public long extractTimestamp(OdsData odsData, long l) {
                        return odsData.getMsgTime().longValue();
                    }
                })
        ).keyBy(data -> data.getVin());

        /**
         * 一次充放电循环状态匹配
         */
        Pattern<OdsData, OdsData> chargePattren = Pattern.<OdsData>begin("charge").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) {
                return "1".equals(data.getChargeStatus());
            }
        }).oneOrMore().consecutive().greedy().next("uncharge").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) throws Exception {
                return "0".equals(data.getChargeStatus());
            }
        }).oneOrMore().consecutive().greedy().next("nextCharge").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData value, Context<OdsData> ctx) throws Exception {
                return "1".equals(value.getChargeStatus());
            }
        }).times(1);


        SingleOutputStreamOperator<chargeAndDisChargeInfo> chargeCircle = CEP.pattern(dataStream, chargePattren).select(new PatternSelectFunction<OdsData, chargeAndDisChargeInfo>() {
            @Override
            public chargeAndDisChargeInfo select(Map<String, List<OdsData>> pattern) throws Exception {
                List<OdsData> charge = pattern.get("charge"); // 首次充电
                List<OdsData> disCharge = pattern.get("uncharge"); // 未充电
                chargeAndDisChargeInfo info = new chargeAndDisChargeInfo();
                info.setChargeStartTime(charge.get(0).getMsgTime());
                info.setChargeEndTime(charge.get(charge.size() - 1).getMsgTime());
                info.setDisChargeStartTime(disCharge.get(0).getMsgTime());
                info.setDisChargeEndTime(disCharge.get(charge.size() - 1).getMsgTime());
                info.setVin(charge.get(0).getVin());
                return info;
            }
        }).keyBy(data -> data.getVin()).filter(new RichFilterFunction<chargeAndDisChargeInfo>() {

            ValueState<chargeAndDisChargeInfo> state = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<chargeAndDisChargeInfo>("info", chargeAndDisChargeInfo.class));
            }

            @Override
            public void close() throws Exception {
                state.clear();
            }

            @Override
            public boolean filter(chargeAndDisChargeInfo data) throws Exception {
                if (state.value() == null || state.value().getDisChargeEndTime() < data.getChargeEndTime() && state.value().getChargeStartTime() > data.getChargeStartTime()) {
                    state.update(data);
                    return true;
                }
                return false;
            }
        });
        chargeCircle.print("输出结果");

        env.execute("ChargeAndStartupMonitor");
    }
}



