package com.adc.da.test;

import com.adc.da.bean.OdsData;
import com.adc.da.functions.ChargeSinkFunction;
import com.adc.da.functions.EventFilterFunction;
import com.adc.da.functions.ShellRichSink;
import com.adc.da.util.CommonUtil;
import com.adc.da.util.ShellUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.sound.midi.Soundbank;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/*import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;*/

/**
 * 监控充电,放电,充放电循环,静置状态和行驶状态结束,然后触发计算
 */
public class TestChargeData {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = CommonUtil.initEnvironment();

        Properties shellConfig = CommonUtil.loadProperties("config/shell.properties");
        Properties odsDataConfig = CommonUtil.loadProperties("config/odsTopic.properties");
        // 设置并行度
        env.setParallelism(Integer.parseInt(shellConfig.get("monitor_parallelism").toString()));
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 创建数据源,提取水位线并设置WaterMark的延时
        KeyedStream<OdsData, String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("computeResult", new SimpleStringSchema(), odsDataConfig)).map(new MapFunction<String, OdsData>() {
            //KeyedStream<OdsData, String> dataStream = env.socketTextStream("hadoop32", 7777).map(new MapFunction<String, OdsData>() {
            @Override
            public OdsData map(String data) {
                OdsData ods = new OdsData();
                JSONObject obj = JSON.parseObject(data, JSONObject.class);
                double soc = Double.parseDouble(obj.getString("soc"));
                ods.setVin(obj.getString("vin"));

                ods.setMsgTime(CommonUtil.dateToTimeStamp(obj.getString("msgTime")));
                //ods.setMsgTime(Long.parseLong(obj.getString("msgTime")));
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
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OdsData>forBoundedOutOfOrderness(Duration.ofSeconds(120))
                .withTimestampAssigner(new SerializableTimestampAssigner<OdsData>() {
                    @Override
                    public long extractTimestamp(OdsData odsData, long l) {
                        return odsData.getMsgTime();
                    }
                })
        ).keyBy(data -> data.getVin());


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
                    if (("0".equals(value.getChargeStatus()))&& "1".equals(value.getStartupStatus())) {
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
        runStream.map(new RichMapFunction<OdsData[], String>() {
            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public String map(OdsData[] odsData) throws Exception {

                System.out.println(odsData[0].getMsgTime() + "  " + odsData[1].getMsgTime());
                return sdf.format(odsData[0].getMsgTime()) + "   " + sdf.format(odsData[1].getMsgTime());
            }
        }).print();

        env.execute("ChargeAndStartupMonitor");
    }
}




