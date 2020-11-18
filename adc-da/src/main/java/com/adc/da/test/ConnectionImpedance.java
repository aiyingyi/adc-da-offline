package com.adc.da.test;

import ch.ethz.ssh2.Connection;
import com.adc.da.bean.OdsData;
import com.adc.da.functions.ChargeSinkFunction;
import com.adc.da.functions.EventFilterFunction;
import com.adc.da.util.CommonUtil;
import com.adc.da.util.ShellUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/*import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;*/

/**
 * 测试connection_impedance
 */
public class ConnectionImpedance {

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
        //chargeStream.map(ods -> ods[0].getMsgTime() + "       " + ods[1].getMsgTime()).print("charge------");
        chargeStream.map(new RichMapFunction<OdsData[], String>() {

            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public String map(OdsData[] odsData) throws Exception {
                return sdf.format(odsData[0].getMsgTime()) + "--" + sdf.format(odsData[1].getMsgTime());
            }
        }).print();

        chargeStream.map(ods -> ods[0].getMsgTime() + " " + ods[1].getMsgTime()).print("-------");

        chargeStream.keyBy(data -> data[0].getVin()).addSink(new RichSinkFunction<OdsData[]>() {

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
            public void invoke(OdsData[] value, SinkFunction.Context context) throws Exception {
                // 1. 执行充电方式，电量以及最大最低电压单体频次脚本
                ShellUtil.exec(conn, shellConfig.getProperty("connection_impedance") + " " + value[0].getVin() + " " + value[0].getMsgTime() + " " + value[1].getMsgTime());
            }
        });
        env.execute("ChargeAndStartupMonitor222");
    }
}




