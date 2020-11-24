package com.adc.da.app;

import com.adc.da.bean.OdsData;
import com.adc.da.bean.chargeAndDisChargeInfo;
import com.adc.da.functions.ChargeSinkFunction;
import com.adc.da.functions.EventFilterFunction;
import com.adc.da.functions.HighSelfDischargeEsSink;
import com.adc.da.functions.ShellRichSink;
import com.adc.da.util.CommonUtil;
import com.adc.da.util.ShellUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/*import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;*/


/**
 * 监控充电,放电,充放电循环,静置状态和行驶状态结束,然后触发计算
 */
public class ChargeAndStartupMonitor2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = CommonUtil.initEnvironment();

        Properties shellConfig = CommonUtil.loadProperties("config/shell.properties");
        Properties odsDataConfig = CommonUtil.loadProperties("config/odsTopic.properties");

        // 设置并行度
        env.setParallelism(Integer.parseInt(shellConfig.get("monitor_parallelism").toString()));

        env.setParallelism(10);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建数据源,提取水位线并设置WaterMark的延时
        KeyedStream<OdsData, String> dataStream = env.addSource(new FlinkKafkaConsumer<String>(odsDataConfig.getProperty("topic"), new SimpleStringSchema(), odsDataConfig)).map(new MapFunction<String, OdsData>() {
            //KeyedStream<OdsData, String> dataStream = env.socketTextStream("hadoop32", 7777).map(new MapFunction<String, OdsData>() {
            @Override
            public OdsData map(String data) {
                OdsData ods = new OdsData();
                JSONObject obj = JSON.parseObject(data, JSONObject.class);
                double soc = Double.parseDouble(obj.getString("soc"));
                ods.setVin(obj.getString("vin"));
                ods.setMsgTime(CommonUtil.dateToTimeStamp(obj.getString("msgTime")));
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
                ods.setTotalCurrent(obj.getDouble("totalCurrent"));
                ods.setBatteryType(obj.getString("batteryType"));
                return ods;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OdsData>forBoundedOutOfOrderness(Duration.ofSeconds(60))
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
                return ("1".equals(data.getChargeStatus())) && (data.getTotalCurrent() < 0);
            }
        }).oneOrMore().consecutive().greedy().next("uncharge").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, Context<OdsData> context) throws Exception {
                return "2".equals(data.getChargeStatus());
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
                //    过滤掉极短时间的充电
            }
        }).filter(new FilterFunction<OdsData[]>() {
            @Override
            public boolean filter(OdsData[] odsData) throws Exception {
                if (odsData[1].getMsgTime() - odsData[0].getMsgTime() > 120000) {
                    return true;
                }
                return false;
            }
        });

        /**
         * 1. 充电压差扩大模型算法调用（10次充电）
         * 2. 充电完成后调用:
         *  1) 电池包衰减预警模型
         *  2) 执行充电方式，电量以及最大最低电压单体频次脚本
         *  3) 连接阻抗大模型算法
         */
        chargeStream.keyBy(data -> data[0].getVin()).addSink(new ChargeSinkFunction(10, shellConfig));

        /**
         * 行驶工况检测
         */
        SingleOutputStreamOperator<OdsData[]> runStream = dataStream.process(new KeyedProcessFunction<String, OdsData, OdsData[]>() {
            ValueState<OdsData> startOds = null;  // 开始行驶数据
            ValueState<OdsData> endOds = null;    // 行驶结束数据

            @Override
            public void open(Configuration parameters) throws Exception {
                startOds = getRuntimeContext().getState(new ValueStateDescriptor<OdsData>("startOds", OdsData.class));
                endOds = getRuntimeContext().getState(new ValueStateDescriptor<OdsData>("endOds", OdsData.class));
            }

            @Override
            public void processElement(OdsData value, Context ctx, Collector<OdsData[]> out) throws Exception {
                // 非充电并且是启动状态
                if (startOds.value() == null) {
                    if (("0".equals(value.getChargeStatus())) && "1".equals(value.getStartupStatus())) {
                        startOds.update(value);
                    }
                } else {
                    if (!("0".equals(value.getStartupStatus()) && "停车档".equals(value.getGearStatus()))) {
                        if (startOds.value() == null) {
                            startOds.update(value);
                        } else {
                            endOds.update(value);
                        }
                    } else if (endOds.value() != null) {
                        // 如果大于指定的时间间隔
                        if (value.getMsgTime() - endOds.value().getMsgTime() >= 5 * 60 * 1000) {
                            OdsData[] run = new OdsData[2];
                            run[0] = startOds.value();
                            run[1] = endOds.value();
                            out.collect(run);
                            // 清空状态
                            startOds.clear();
                            endOds.clear();
                        }
                    }
                }
            }
        });

        // 合并充电和行驶状态
        SingleOutputStreamOperator<OdsData[]> chargeAndRunStream = chargeStream.connect(runStream).process(new CoProcessFunction<OdsData[], OdsData[], OdsData[]>() {
            @Override
            public void processElement1(OdsData[] value, Context ctx, Collector<OdsData[]> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void processElement2(OdsData[] value, Context ctx, Collector<OdsData[]> out) throws Exception {
                out.collect(value);
            }
        });
        chargeAndRunStream.keyBy(odsData -> odsData[0].getVin()).addSink(new ShellRichSink<OdsData[]>(shellConfig) {
            @Override
            public void invoke(OdsData[] value, Context context) throws Exception {

                // 单体电压离散度高模型
                ShellUtil.exec(conn, shellConfig.getProperty("cellVolHighDis") + " " + value[0].getVin() + " " + value[0].getMsgTime() + " " + value[1].getMsgTime());
                // 绝缘电阻突降
                ShellUtil.exec(conn, shellConfig.getProperty("resistance_reduce") + " " + value[0].getVin() + " " + value[0].getMsgTime() + " " + value[1].getMsgTime());
                // bms采样异常
                ShellUtil.exec(conn, shellConfig.getProperty("bms_sampling") + " " + value[0].getVin() + " " + value[0].getMsgTime() + " " + value[1].getMsgTime());

                // 模组电压离群
                ShellUtil.exec(conn, shellConfig.getProperty("module_vol_exception") + " " + value[0].getVin() + " " + value[0].getMsgTime() + " " + value[1].getMsgTime());

            }
        });


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

        // todo sink到es还有一部分未完成：电池类型数据源,风险等级变化,
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

        // todo  未完成充放电状态匹配
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
                return "2".equals(data.getChargeStatus());
            }
        }).oneOrMore().consecutive().greedy().followedBy("nextCharge").where(new IterativeCondition<OdsData>() {
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




