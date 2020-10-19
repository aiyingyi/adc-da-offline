package com.adc.da.app;

import com.adc.da.bean.ChargeRecord;
import com.adc.da.bean.OdsData;
import com.adc.da.functions.ChargeSinkFunction;
import com.adc.da.functions.HighSelfDischargeEsSink;
import com.adc.da.util.ComUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
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
import java.util.*;

/**
 * 监控充电完成和行驶结束，触发计算
 */
public class ChargeAndStartupMonitor {
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

        // 监控充电完成
        Pattern<OdsData, OdsData> chargePattren = Pattern.<OdsData>begin("charge").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, IterativeCondition.Context<OdsData> context) {
                return "1".equals(data.getChargeStatus());
            }
        }).oneOrMore().consecutive().greedy().next("uncharge").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) throws Exception {
                return "0".equals(data.getChargeStatus());
            }
        }).times(1);
        //  cep 会将数据按照时间戳进行排序，并且只有watermark>匹配数据的最大的时间戳，才会输出
        //  cep 会根据key进行匹配，不同的key的数据不会相互影响
        SingleOutputStreamOperator<ChargeRecord> chargeStream = CEP.pattern(dataStream, chargePattren).select(new PatternSelectFunction<OdsData, ChargeRecord>() {
            @Override
            public ChargeRecord select(Map<String, List<OdsData>> map) throws Exception {

                List<OdsData> chargeList = map.get("charge");
                OdsData o1 = chargeList.get(0);
                OdsData o2 = chargeList.get(chargeList.size() - 1);
                return new ChargeRecord(o1.getVin(), o1.getMsgTime(), o2.getMsgTime(), o1.getSoc(), o2.getSoc());
            }
            // 将同一充电过程中的其他匹配给过滤掉，只保留第一条充电数据到第一条非充电数据的匹配
        }).keyBy(chargeRecord -> chargeRecord.getVin()).filter(new RichFilterFunction<ChargeRecord>() {
            ValueState<ChargeRecord> chargeState = null;
            ChargeRecord value = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                chargeState = getRuntimeContext().getState(new ValueStateDescriptor("chargeStartAndEnd", ChargeRecord.class));
            }

            @Override
            public boolean filter(ChargeRecord cr) throws Exception {
                value = chargeState.value();
                if (value == null || value.getEndTime() != cr.getEndTime()) {
                    chargeState.update(cr);
                    return true;
                }
                return false;
            }

            @Override
            public void close() throws Exception {
                chargeState.clear();
            }
        });

        /**
         * 1. 充电压差扩大模型算法调用（10次充电）
         * 2. 充电完成后调用:
         *  1) 电池包衰减预警模型
         *  2) 单体电池离散度高(充电、行驶结束后)
         *  3)执行充电方式，电量以及最大最低电压单体频次脚本
         */

        chargeStream.keyBy(data -> data.getVin()).addSink(new ChargeSinkFunction(2, shellConfig));


        /**
         *判断车辆是否静置半天
         */
        Pattern<OdsData, OdsData> staticPattren = Pattern.<OdsData>begin("start").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, IterativeCondition.Context<OdsData> context) {
                return true;
            }
        }).next("end").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) throws Exception {
                OdsData start = context.getEventsForPattern("start").iterator().next();
                double gap = (data.getMsgTime() - start.getMsgTime()) / (1000 * 3600 * 24.0);
                return gap > 0.5 && start.getSoc() == data.getSoc() ? true : false;
            }
        });

        // 获取静置状态的两端的两条数据
        SingleOutputStreamOperator<OdsData[]> staticStream = CEP.pattern(dataStream, staticPattren).select(new PatternSelectFunction<OdsData, OdsData[]>() {
            @Override
            public OdsData[] select(Map<String, List<OdsData>> pattern) throws Exception {
                OdsData[] startAndEnd = new OdsData[2];
                startAndEnd[0] = pattern.get("start").get(0);
                startAndEnd[1] = pattern.get("end").get(0);
                return startAndEnd;
            }
        });

        staticStream.addSink(HighSelfDischargeEsSink.getEsSink());

        env.execute("ChargeAndStartupMonitor");
    }
}



