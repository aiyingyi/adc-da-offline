package com.adc.da.app;

import com.adc.da.bean.OdsData;
import com.adc.da.util.ComUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.IOException;
import java.sql.Time;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Test {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = ComUtil.initEnvironment();
        // 设置并行度
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties shellConfig = ComUtil.loadProperties("config/shell.properties");
        Properties odsDataConfig = ComUtil.loadProperties("config/chargeMonitor.properties");


        // 创建数据源
        //KeyedStream<Tuple3<String, Long, String>, String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>(odsDataConfig.getProperty("topic"), new SimpleStringSchema(), odsDataConfig)).map(new MapFunction<String, Tuple3<String, Long, String>>() {
        KeyedStream<Tuple3<String, Long, String>, String> dataStream = env.socketTextStream("hadoop32", 7777).map(new MapFunction<String, Tuple3<String, Long, String>>() {

            @Override
            public Tuple3<String, Long, String> map(String s) throws Exception {
                JSONObject obj = JSON.parseObject(s, JSONObject.class);
                return new Tuple3(obj.getString("vin"), Long.parseLong(obj.getString("msgTime")), obj.getString("chargeStatus"));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, String> ele, long l) {
                        return ele.f1;
                    }
                })
        ).keyBy(t -> t.f0);

        //Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> chargePattren = Pattern.<Tuple3<String, Long, String>>begin("charge").where(new IterativeCondition<Tuple3<String, Long, String>>() {
        //    @Override
        //    public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) {
        //        return "1".equals(data.f2);
        //    }
        //}).timesOrMore(1).next("uncharge").where(new IterativeCondition<Tuple3<String, Long, String>>() {
        //    @Override
        //    public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) throws Exception {
        //        return "0".equals(data.f2);
        //    }
        //});
        //Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> chargePattren = Pattern.<Tuple3<String, Long, String>>begin("charge").where(new IterativeCondition<Tuple3<String, Long, String>>() {
        //    @Override
        //    public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) {
        //        return "1".equals(data.f2);
        //    }
        //}).oneOrMore().greedy().next("uncharge").where(new IterativeCondition<Tuple3<String, Long, String>>() {
        //    @Override
        //    public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) throws Exception {
        //        return "0".equals(data.f2);
        //    }
        //});

        //Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> chargePattren = Pattern.<Tuple3<String, Long, String>>begin("charge").where(new IterativeCondition<Tuple3<String, Long, String>>() {
        //    @Override
        //    public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) {
        //        return "1".equals(data.f2);
        //    }
        //}).timesOrMore(1).consecutive().greedy().until(
        //        new IterativeCondition<Tuple3<String, Long, String>>() {
        //            @Override
        //            public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) throws Exception {
        //                return "0".equals(data.f2);
        //            }
        //        }
        //);



        Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> chargePattren = Pattern.<Tuple3<String, Long, String>>begin("charge").where(new IterativeCondition<Tuple3<String, Long, String>>() {
            @Override
            public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) {
                return "1".equals(data.f2);
            }
        }).next("uncharge").where(new IterativeCondition<Tuple3<String, Long, String>>() {
            @Override
            public boolean filter(Tuple3<String, Long, String> data, Context<Tuple3<String, Long, String>> context) throws Exception {
                return "0".equals(data.f2);
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Long>> charge = CEP.pattern(dataStream, chargePattren).select(new PatternSelectFunction<Tuple3<String, Long, String>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> select(Map<String, List<Tuple3<String, Long, String>>> map) throws Exception {


                List<Tuple3<String, Long, String>> chargeList = map.get("charge");
                Tuple3<String, Long, String> tuple = chargeList.get(0);
                System.out.println(chargeList.size());
                return new Tuple2(tuple.f0, tuple.f1);
            }
        });

        charge.print("charge");
        //dataStream.print("dataStream");


        env.execute("test");


    }
}
