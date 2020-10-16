package com.adc.da.app;

import ch.ethz.ssh2.Connection;
import com.adc.da.bean.ChargeRecord;
import com.adc.da.bean.OdsData;
import com.adc.da.functions.ChargeVolDiffExpProcessFunction;
import com.adc.da.util.ComUtil;
import com.adc.da.util.ShellUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import scala.actors.threadpool.Arrays;

import java.awt.print.Printable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
        Properties odsDataConfig = ComUtil.loadProperties("config/chargeMonitor.properties");
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
                List<OdsData> unchargeList = map.get("uncharge");

                OdsData o1 = chargeList.get(0);
                OdsData o2 = unchargeList.get(0);
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
         * 1. 充电压差扩大模型算法调用
         * 2. 充电完成后调用:1. 电池包衰减预警模型,2. 执行充电方式，电量以及最大最低电压单体频次脚本
         */

        chargeStream.keyBy(data -> data.getVin()).addSink(new ChargeSinkFunction(10, shellConfig));

        env.execute("ChargeAndRunMonitor");
    }
}

class ChargeSinkFunction extends RichSinkFunction<ChargeRecord> {

    // 充电压差扩大模型的充电次数窗口大小
    private int windowSize = 0;

    // shell环境配置以及脚本执行路径
    private Properties shellConfig = null;

    public ChargeSinkFunction(int windowSize, Properties shellConfig) {
        this.windowSize = windowSize;
        this.shellConfig = shellConfig;
    }

    // 充电次数
    ValueState<Integer> chargeTimes = null;
    // 记录符合条件的10次充电起始和结束时间
    ValueState<long[]> chargeStartAndEnd = null;
    Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        chargeTimes = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("chargeTimes", Integer.class));
        chargeTimes.update(0);
        chargeStartAndEnd = getRuntimeContext().getState(new ValueStateDescriptor("chargeStartAndEnd", long[].class));
        chargeStartAndEnd.update(new long[2 * windowSize]);
        conn = ShellUtil.getConnection(shellConfig.getProperty("userName"), shellConfig.getProperty("passWord"), shellConfig.getProperty("ip"), Integer.parseInt(shellConfig.getProperty("port")));
    }

    @Override
    public void close() throws Exception {
        chargeTimes.clear();
        chargeStartAndEnd.clear();
    }

    @Override
    public void invoke(ChargeRecord value, Context context) throws Exception {

        // 1. 执行充电方式，电量以及最大最低电压单体频次脚本
        ShellUtil.exec(conn, shellConfig.getProperty("chargeStyleElectricityFrequencyPath") + " " + value.getStartTime() + " " + value.getEndTime() + " " + value.getVin());

        // 2. TODO 电池包衰减预警模型
        if (value.getEndSoc() - value.getStartSoc() > 40) {
            // ShellUtil.exec(conn) );
        }
        // 3.充电压差扩大模型
        if (value.getStartSoc() <= 80 && value.getEndSoc() >= 80) {
            chargeTimes.update(chargeTimes.value() + 1);
            // 获取之前10次的充电时间
            long[] arr = chargeStartAndEnd.value();

            // 相当于一个循环队列，
            int index = (chargeTimes.value() % windowSize + 1) * 2 - 1;
            arr[index - 1] = value.getStartTime();
            arr[index] = value.getEndTime();
            // 状态更新
            chargeStartAndEnd.update(arr);
            // 假如充电次数超过了10次
            if (chargeTimes.value() >= 10) {
                long[] timeArray = chargeStartAndEnd.value();
                // 时间数组按照值进行排序，是对充电进行排序
                Arrays.sort(timeArray);

                // 拼接10次充电的时间戳字符串
                String shellArgs = " ";
                for (long time : timeArray) {
                    shellArgs = shellArgs + " " + time + " ";
                }
                shellArgs = shellArgs + value.getVin();
                // 传入执行脚本的路径和时间参数,以及vin码
                ShellUtil.exec(conn, shellConfig.getProperty("chargeVolDiffExtendModulePath") + shellArgs);


            }
        }
    }
}


