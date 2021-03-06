package com.adc.da.app;

import ch.ethz.ssh2.Connection;
import com.adc.da.functions.ChargeVolDiffExpProcessFunction;
import com.adc.da.util.ComUtil;
import com.adc.da.util.ShellUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Properties;

/**
 * 对充电数据进行监控，满足充电条件则执行脚本
 * 1 充电压差扩大模型算法
 * 2 电池包衰减预警模型，监测充电完成且充电电量大于40%
 */
public class ChargeApplication {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = ComUtil.initEnvironment();
        // 设置并行度
        env.setParallelism(10);

        Properties shellConfig = ComUtil.loadProperties("config/shell.properties");
        Properties odsDataConfig = ComUtil.loadProperties("config/chargeMonitor.properties");
        // 需求1 充电压差扩大模型算法
        // 创建数据源
        SingleOutputStreamOperator<Tuple4<String, String, Double, String>> dataStream = env.addSource(new FlinkKafkaConsumer011<String>(odsDataConfig.getProperty("topic"), new SimpleStringSchema(), odsDataConfig)).map(new MapFunction<String, Tuple4<String, String, Double, String>>() {
            @Override
            public Tuple4<String, String, Double, String> map(String s) {
                JSONObject obj = JSON.parseObject(s, JSONObject.class);
                double soc = Double.parseDouble(obj.getString("soc").substring(0, obj.getString("soc").length() - 1));
                return new Tuple4(obj.getString("vin"), obj.getString("chargeStatus"), soc, obj.getString("msgTime"));
            }
        });
        dataStream.keyBy(data -> data.f0)
                .process(new ChargeVolDiffExpProcessFunction(Integer.parseInt(odsDataConfig.getProperty("chargeTime"))))
                .addSink(new RichSinkFunction<Tuple2<String, String[]>>() {
                    Connection conn = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        conn = ShellUtil.getConnection(shellConfig.getProperty("userName"), shellConfig.getProperty("passWord"), shellConfig.getProperty("ip"), Integer.parseInt(shellConfig.getProperty("port")));
                    }

                    @Override
                    public void close() throws Exception {
                        if (conn != null) {
                            conn.close();
                        }
                    }

                    @Override
                    public void invoke(Tuple2<String, String[]> value, Context context) throws Exception {
                        // 拼接10次充电的时间戳字符串
                        String shellArgs = " ";
                        for (String elem : value.f1) {
                            shellArgs = shellArgs + " " + elem + " ";
                        }
                        shellArgs = shellArgs + value.f0;
                        // 传入执行脚本的路径和时间参数,以及vin码
                        ShellUtil.exec(conn, shellConfig.getProperty("chargeVolDiffExtendModulePath") + shellArgs);
                    }
                });
        // 需求2：电池包衰减预警模型，监测充电完成且充电电量大于40%
        dataStream.keyBy(data -> data.f0).process(new KeyedProcessFunction<String, Tuple4<String, String, Double, String>, Tuple5<String, String, String, Double, Double>>() {
            // 保存是否充电状态
            ValueState<Boolean> isCharge = null;
            ValueState<String> startTime = null;
            ValueState<String> endTime = null;
            ValueState<Double> startSoc = null;
            ValueState<Double> endSoc = null;

            @Override
            public void open(Configuration parameters) {

                isCharge = getRuntimeContext().getState(new ValueStateDescriptor("isCharge", Boolean.class));
                startTime = getRuntimeContext().getState(new ValueStateDescriptor("startTime", String.class));
                endTime = getRuntimeContext().getState(new ValueStateDescriptor("endTime", String.class));
                // 充电电量
                startSoc = getRuntimeContext().getState(new ValueStateDescriptor("startSoc", Double.class));
                endSoc = getRuntimeContext().getState(new ValueStateDescriptor("endSoc", Double.class));

            }

            @Override
            public void processElement(Tuple4<String, String, Double, String> value, Context ctx, Collector<Tuple5<String, String, String, Double, Double>> out) throws Exception {

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
                    if (isCharge.value() == true && startTime.value().compareTo(value.f3) < 0) { // 之前处于充电状态
                        isCharge.update(false);
                        if (endSoc.value() - startSoc.value() > 40) {
                            out.collect(new Tuple5<>(vin, startTime.value(), endTime.value(), startSoc.value(), endSoc.value()));
                        }
                    }
                }
            }
        }).addSink(new RichSinkFunction<Tuple5<String, String, String, Double, Double>>() {
            Connection conn = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = ShellUtil.getConnection(shellConfig.getProperty("userName"), shellConfig.getProperty("passWord"), shellConfig.getProperty("ip"), Integer.parseInt(shellConfig.getProperty("port")));
            }

            @Override
            public void close() throws Exception {
                if (conn != null) {
                    conn.close();
                }
            }

            @Override
            public void invoke(Tuple5<String, String, String, Double, Double> value, Context context) throws Exception {
                ShellUtil.exec(conn, shellConfig.getProperty("chargeCapacityPath") + " " + value.f0 + " " + value.f1 + " " + value.f2 + " " + value.f3 + " " + value.f3);
            }
        });

        env.execute("charge monitor");
    }
}



