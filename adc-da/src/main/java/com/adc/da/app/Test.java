package com.adc.da.app;

import com.adc.da.bean.OdsData;
import com.adc.da.util.ComUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = ComUtil.initEnvironment();
        // 设置并行度
        env.setParallelism(1);

        DataStreamSource<String> hadoop32 = env.socketTextStream("hadoop32", 7777);

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("es29", 9200, "http"));
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("vin", element);
                        System.out.println(json);
                        return Requests.indexRequest()
                                .index("test")
                                .type("test")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);


        hadoop32.addSink(esSinkBuilder.build());

        env.execute("edqwd");


    }
}
