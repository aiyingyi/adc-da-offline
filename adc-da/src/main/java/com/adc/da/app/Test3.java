package com.adc.da.app;

import com.adc.da.bean.ChargeRecord;
import com.adc.da.bean.EventInfo;
import com.adc.da.bean.OdsData;
import com.adc.da.bean.chargeAndDisChargeInfo;
import com.adc.da.functions.ChargeSinkFunction;
import com.adc.da.functions.HighSelfDischargeEsSink;
import com.adc.da.functions.ShellRichSink;
import com.adc.da.util.ComUtil;
import com.adc.da.util.PlatformAlgorithm;
import com.adc.da.util.ShellUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;

public class Test3 {

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
         * 电芯自放电大模型算法 判断车辆是否静置半天
         */
        Pattern<OdsData, OdsData> staticPattren = Pattern.<OdsData>begin("start").where(new IterativeCondition<OdsData>() {
            @Override
            public boolean filter(OdsData data, IterativeCondition.Context<OdsData> context) {
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
        }).keyBy(data -> data[0].getVin()).filter(new RichFilterFunction<OdsData[]>() {

            ValueState<OdsData[]> staticState = null;
            OdsData[] value = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                staticState = getRuntimeContext().getState(new ValueStateDescriptor<OdsData[]>("static", OdsData[].class));
            }

            @Override
            public void close() throws Exception {
                staticState.clear();
            }

            @Override
            public boolean filter(OdsData[] odsData) throws Exception {
                // 获取状态值
                value = staticState.value();
                if (value == null || value[1].getMsgTime() != odsData[1].getMsgTime()) {
                    staticState.update(odsData);
                    return true;
                }
                return false;
            }
        });

        staticStream.print();

        // todo sink到es还有一部分未完成
        staticStream.keyBy(data -> data[0].getVin()).addSink(HighSelfDischargeEsSinkvvvv.getEsSink());


        env.execute();


    }
}

class HighSelfDischargeEsSinkvvvv {

    // 获取es sink
    // todo  将配置信息和index信息放在配置文件中
    public static ElasticsearchSink<OdsData[]> getEsSink() {
        // 创建 es sink
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("es29", 9200, "http"));

        ElasticsearchSink.Builder<OdsData[]> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<OdsData[]>() {
                    public IndexRequest createIndexRequest(OdsData[] element) throws ParseException {
                        // 创建日期，电压数组
                        long[] dt = {element[0].getMsgTime(), element[1].getMsgTime()};
                        double cellVol[][] = new double[2][];
                        cellVol[0] = element[0].getCellVoltage();
                        cellVol[1] = element[1].getCellVoltage();
                        // 假如发生预警
                        if (new PlatformAlgorithm().selfDischargeBig(dt, cellVol) == 0) {

                            //  todo 电池类型的获取，风险等级的变更  从Mysql中获取

                            Map<String, Object> json = new HashMap<>();
                            json.put("vin", element[0].getVin());
                            json.put("vehicleType", element[0].getVehicleType());
                            json.put("enterprise", element[1].getEnterprise());
                            json.put("licensePlate", element[1].getLicensePlate());
                            json.put("batteryType", null);
                            json.put("riskLevel", "1");
                            json.put("province", element[1].getProvince());
                            json.put("warningStartTime", element[0].getMsgTime() + "");
                            json.put("warningEndTime", element[1].getMsgTime() + "");
                            json.put("warningType", "电芯自放电大");
                            json.put("loseEfficacyType", "电芯自放电大");
                            json.put("reviewStatus", "1");
                            json.put("reviewResult", null);
                            json.put("reviewUser", null);

                            System.out.println(json);

                            return Requests.indexRequest()
                                    .index("warning")
                                    .type("warning")
                                    .source(json);
                        }
                        return null;

                    }

                    @Override
                    public void process(OdsData[] element, RuntimeContext ctx, RequestIndexer indexer) {
                        try {
                            indexer.add(createIndexRequest(element));
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);
        return esSinkBuilder.build();
    }
}


