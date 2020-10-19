package com.adc.da.functions;

import com.adc.da.bean.OdsData;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

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
                    public IndexRequest createIndexRequest(OdsData[] element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("vin", element[0].getVin());
                        json.put("startTime", element[0].getMsgTime() + "");
                        json.put("endTime", element[1].getMsgTime() + "");

                        // todo 在此处执行预警算法

                        //json.put("isWarning", "1");

                        return Requests.indexRequest()
                                .index("high_self_discharge")
                                .type("high_self_discharge")
                                .source(json);
                    }

                    @Override
                    public void process(OdsData[] element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);
        return esSinkBuilder.build();
    }
}
