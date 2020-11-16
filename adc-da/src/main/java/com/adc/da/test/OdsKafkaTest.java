package com.adc.da.test;

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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
/**
 * 监控充电,放电,充放电循环,静置状态和行驶状态结束,然后触发计算
 */
public class OdsKafkaTest {


    public static void main2(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(CommonUtil.loadProperties("config/odsTopic.properties"));
        consumer.subscribe(Arrays.asList("data"));

        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(500);

            for (ConsumerRecord<String, String> str : poll) {


                System.out.println(str.value());

            }
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = CommonUtil.initEnvironment();

        Properties odsDataConfig = CommonUtil.loadProperties("config/odsTopic.properties");

        // 设置并行度
        env.setParallelism(1);

        // 创建数据源,提取水位线并设置WaterMark的延时
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("data", new SimpleStringSchema(), odsDataConfig));


        dataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String data) throws Exception {
                OdsData ods = new OdsData();
                JSONObject obj = JSON.parseObject(data, JSONObject.class);
                //double soc = Double.parseDouble(obj.getString("soc"));
                ods.setVin(obj.getString("vin"));
                ods.setMsgTime(Long.parseLong(obj.getString("msgTime")));
                //ods.setMsgTime(Long.parseLong(obj.getString("msgTime")));
                ods.setSpeed(obj.getDouble("speed"));
                ods.setStartupStatus(obj.getString("startupStatus"));
                ods.setGearStatus(obj.getString("gearStatus"));
                ods.setChargeStatus(obj.getString("chargeStatus"));
                //ods.setSoc(soc);
                //ods.setOdo(obj.getDouble("odo"));
                // 解析电压数组
                //String cellVoltage = obj.getString("cellVoltage");
                //double[] vols = Arrays.stream(cellVoltage.substring(1, cellVoltage.length() - 2).split(",")).mapToDouble(vol -> Double.parseDouble(vol)).toArray();
                //ods.setCellVoltage(vols);
                //ods.setVehicleType(obj.getString("vehicleType"));
                //ods.setEnterprise(obj.getString("enterprise"));
                //ods.setLicensePlate(obj.getString("licensePlate"));
                //ods.setProvince(obj.getString("province"));
                return JSON.toJSONString(ods, false);
            }
        }).writeAsText("C:\\Users\\13099\\Desktop\\data.txt");

        env.execute("ChargeAndStartupMonitor");
    }
}




