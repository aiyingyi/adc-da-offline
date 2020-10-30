package com.adc.da.app;

import com.adc.da.bean.ChargeRecord;
import com.adc.da.bean.EventInfo;
import com.adc.da.bean.OdsData;
import com.adc.da.bean.chargeAndDisChargeInfo;
import com.adc.da.functions.ChargeSinkFunction;
import com.adc.da.functions.EventFilterFunction;
import com.adc.da.functions.HighSelfDischargeEsSink;
import com.adc.da.functions.ShellRichSink;
import com.adc.da.util.ComUtil;
import com.adc.da.util.ShellUtil;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 监控充电,放电,静置状态和行驶结束，触发计算
 */
public class ChargeAndStartupMonitor0000000 {
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

                ods.setVehicleType(obj.getString("vehicleType"));
                ods.setEnterprise(obj.getString("enterprise"));
                ods.setLicensePlate(obj.getString("licensePlate"));
                ods.setProvince(obj.getString("province"));

                return ods;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OdsData>forBoundedOutOfOrderness(Duration.ofSeconds(6))
                .withTimestampAssigner(new SerializableTimestampAssigner<OdsData>() {
                    @Override
                    public long extractTimestamp(OdsData odsData, long l) {
                        return odsData.getMsgTime();
                    }
                })
        ).keyBy(data -> data.getVin());


        /**
         * 充电完成状态匹配
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
        }).times(1);

        //  cep 会将数据按照时间戳进行排序，并且只有watermark>匹配数据的最大的时间戳，才会输出
        //  cep 会根据key进行匹配，不同的key的数据不会相互影响
        SingleOutputStreamOperator<OdsData[]> chargeStream = CEP.pattern(dataStream, chargePattren).select(new PatternSelectFunction<OdsData, OdsData[]>() {
            @Override
            public OdsData[] select(Map<String, List<OdsData>> map) throws Exception {

                List<OdsData> chargeList = map.get("charge");
                OdsData o1 = chargeList.get(0);
                OdsData o2 = chargeList.get(chargeList.size() - 1);
                return new OdsData[]{o1, o2};
            }
            // 将同一充电过程中的其他匹配给过滤掉，只保留第一条充电数据到第一条非充电数据的匹配
        }).keyBy(data -> data[0].getVin()).filter(new EventFilterFunction() {


            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<OdsData[]>("chargeState", OdsData[].class));
            }
        });

        /**
         * 行驶工况匹配
         */

        Pattern<OdsData, OdsData> runPattren = Pattern.<OdsData>begin("run").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) {

                return data.getSpeed() <= 0 ? true : false;

            }
        }).oneOrMore().consecutive().greedy().next("unRun").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) throws Exception {
                return data.getSpeed() > 0 ? true : false;
            }
        }).times(1);

        //  cep 会将数据按照时间戳进行排序，并且只有watermark>匹配数据的最大的时间戳，才会输出
        //  cep 会根据key进行匹配，不同的key的数据不会相互影响
        SingleOutputStreamOperator<OdsData[]> runStream = CEP.pattern(dataStream, chargePattren).select(new PatternSelectFunction<OdsData, OdsData[]>() {
            @Override
            public OdsData[] select(Map<String, List<OdsData>> map) throws Exception {
                List<OdsData> runList = map.get("run");
                OdsData o1 = runList.get(0);
                OdsData o2 = runList.get(runList.size() - 1);
                return new OdsData[]{o1, o2};
            }
            // 将同一行驶过程中的其他匹配给过滤掉，只保留第一条数据到最后一条数据的匹配
        }).keyBy(data -> data[0].getVin()).filter(new EventFilterFunction() {
            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<OdsData[]>("runState", OdsData[].class));
            }
        });

        // 合并充电和行驶状态

        chargeStream.connect(runStream).process()




        /**
         * 1. 充电压差扩大模型算法调用（10次充电）
         * 2. 充电完成后调用:
         *  1) 电池包衰减预警模型
         *  2) 单体电池离散度高(充电、行驶结束后)
         *  3) 执行充电方式，电量以及最大最低电压单体频次脚本
         */

        chargeStream.keyBy(data -> data[0].getVin()).addSink(new ChargeSinkFunction(10, shellConfig));

        /**
         * 电芯自放电大模型算法 判断车辆是否静置半天
         */
        Pattern<OdsData, OdsData> staticPattren = Pattern.<OdsData>begin("start").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) {
                if (data.getSpeed() == 0.0 && "0".equals(data.getStartupStatus())) {
                    return true;
                }
                return false;
            }
        }).oneOrMore().consecutive().greedy().next("end").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) throws Exception {
                if (data.getSpeed() != 0.0 || !"0".equals(data.getStartupStatus())) {
                    OdsData start = context.getEventsForPattern("start").iterator().next();
                    double gap = (data.getMsgTime() - start.getMsgTime()) / (1000 * 3600 * 24.0);
                    return gap > 0.5 && (start.getSoc() - data.getSoc() >= -5 && start.getSoc() - data.getSoc() <= 5) ? true : false;
                }
                return false;
            }
        });

        // 获取静置状态的两端的两条数据
        SingleOutputStreamOperator<OdsData[]> staticStream = CEP.pattern(dataStream, staticPattren).select(new PatternSelectFunction<OdsData, OdsData[]>() {
            @Override
            public OdsData[] select(Map<String, List<OdsData>> pattern) throws Exception {
                OdsData[] startAndEnd = new OdsData[2];
                List<OdsData> events = pattern.get("start");
                startAndEnd[0] = events.get(0);
                startAndEnd[1] = events.get(events.size() - 1);
                return startAndEnd;
            }
        }).keyBy(data -> data[0].getVin()).filter(new EventFilterFunction() {

            // 设置静置状态
            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<OdsData[]>("staticState", OdsData[].class));

            }
        });

        // todo sink到es还有一部分未完成
        staticStream.keyBy(data -> data[0].getVin()).addSink(HighSelfDischargeEsSink.getEsSink());


        /**
         * 放电状态匹配：上一次充电完成-下次充电开始的时间
         */
        Pattern<OdsData, OdsData> disChargePattren = Pattern.<OdsData>begin("start").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) {
                return "0".equals(data.getChargeStatus());
            }
        }).oneOrMore().consecutive().greedy().next("end").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) throws Exception {
                return "1".equals(data.getChargeStatus());
            }
        });

        SingleOutputStreamOperator<OdsData[]> disChargeStream = CEP.pattern(dataStream, disChargePattren).select(new PatternSelectFunction<OdsData, OdsData[]>() {
            @Override
            public OdsData[] select(Map<String, List<OdsData>> pattern) throws Exception {
                List<OdsData> start = pattern.get("start");
                return new OdsData[]{start.get(0), start.get(start.size() - 1)};
            }
        }).keyBy(data -> data[0].getVin()).filter(new EventFilterFunction() {
            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<OdsData[]>("disChargeState", OdsData[].class));
            }
        });
        // 执行单体电压波动性差异大模型算法脚本
        disChargeStream.keyBy(data -> data[0].getVin()).addSink(new ShellRichSink<OdsData[]>(shellConfig) {

            @Override
            public void invoke(OdsData[] value, Context context) throws Exception {
                ShellUtil.exec(conn, shellConfig.getProperty("volFluctuation") + " " + value[0].getVin() + " " + value[0].getMsgTime() + " " + value[1].getMsgTime());
            }
        });

        /**
         * 一次充放电循环状态匹配
         */

        Pattern<OdsData, OdsData> circlePattren = Pattern.<OdsData>begin("charge").where(new IterativeCondition<OdsData>() {
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

        SingleOutputStreamOperator<chargeAndDisChargeInfo> chargeCircle = CEP.pattern(dataStream, circlePattren).select(new PatternSelectFunction<OdsData, chargeAndDisChargeInfo>() {
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
                state = getRuntimeContext().getState(new ValueStateDescriptor<chargeAndDisChargeInfo>("chargeAndDisCharge", chargeAndDisChargeInfo.class));
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
        chargeCircle.keyBy(data -> data.getVin()).addSink(new ShellRichSink<chargeAndDisChargeInfo>(shellConfig) {
            @Override
            // 执行容量异常脚本
            public void invoke(chargeAndDisChargeInfo value, Context context) throws Exception {
                ShellUtil.exec(conn, shellConfig.getProperty("capacity_anomaly") + " " + value.getVin() + " " + value.getChargeStartTime() + " " + value.getChargeEndTime() + " " + value.getDisChargeStartTime() + " " + value.getDisChargeEndTime());
            }
        });
        env.execute("ChargeAndStartupMonitor");
    }
}



