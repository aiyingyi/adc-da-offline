package com.adc.da.functions;

import com.adc.da.bean.OdsData;
import com.adc.da.util.PlatformAlgorithm;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自放电大算法,处理数据后sink到es
 */
public class HighSelfDischargeEsSink {

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
                        if (new PlatformAlgorithm().selfDischargeBig(dt, cellVol) == 1) {

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
