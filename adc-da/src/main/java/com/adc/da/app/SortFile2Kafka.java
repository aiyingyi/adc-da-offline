package com.adc.da.app;


import com.adc.da.util.CommonUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Collector;

import java.io.File;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * flink 批处理，用于文件合并排序并发送给kafka
 */
public class SortFile2Kafka {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.11.35:9092");
        props.put("zookeeper.connect", "192.168.11.35:2181");
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 5000);//批次大小
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小


        // 获取文件路径
        String sourcePath = "C:\\Users\\13099\\Desktop\\2";//args[0];

        // Todo 定义源文件和平台数据映射字段值    如何在用户配置后获取数据
        String[] attName = new String[]{"VIN", "报文时间", "车速", "车辆状态", "运行模式", "累计里程", "档位", "充电状态",
                "加速踏板行程值", "制动踏板状态", "总电压", "总电流", "SOC", "绝缘电阻", "定位状态", "纬度", "经度",
                "通用报警标志", "单体电池总数", "可充电储能温度探针个数", "单体电池电压", "可充电储能装置温度数据"};
        String[] dataName = new String[]{"vin", "msgTime", "speed", "startupStatus", "runMode", "odo", "gearStatus", "chargeStatus", "aptv", "bptv",
                "totalVoltage", "totalCurrent", "soc", "insulationResistance", "positionStatus", "longitude", "latitude", "failure", "cellNum", "probeNum",
                "cellVoltage", "probeTemperature"};

        // 获取数据的条数，用来开启一个计数窗口
        long recordSize = FileParse.getRecordSize(new File(sourcePath));
        StreamExecutionEnvironment env = CommonUtil.initEnvironment();
        env.setParallelism(1);
        // 从数据源获取数据
        SingleOutputStreamOperator<Map<String, Object>> dataSource = env.addSource(new ExcelSource(sourcePath, attName, dataName));

        SingleOutputStreamOperator<Map<String, Object>> resStream = dataSource.map(new MapFunction<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Map<String, Object> data) throws Exception {
                // 将时间转换成时间戳
                data.put("msgTime", FileParse.dateToStamp(data.get("msgTime").toString()));
                // 对数据进行清洗
                if ("启动".equals(data.get("startupStatus"))) {
                    data.put("startupStatus", "1");
                } else {
                    data.put("startupStatus", "0");
                }
                if ("未充电状态".equals(data.get("chargeStatus"))) {
                    data.put("chargeStatus", "1");
                } else {
                    data.put("chargeStatus", "0");
                }
                if ("纯电".equals(data.get("runMode"))) {
                    data.put("runMode", "1");
                }
                if ("有效".equals(data.get("positionStatus"))) {
                    data.put("positionStatus", "1");
                }
                if ("停车档".equals(data.get("gearStatus"))) {
                    data.put("positionStatus", "1");
                }

                double[] probeTemperature = FileParse.str2DouleArr(data.get("probeTemperature").toString());
                double[] cellVoltage = FileParse.str2DouleArr(data.get("cellVoltage").toString());

                data.put("probeTemperature", probeTemperature);
                data.put("cellVoltage", cellVoltage);
                return data;
            }
        }).setParallelism(8).countWindowAll(recordSize).process(new ProcessAllWindowFunction<Map<String, Object>, Map<String, Object>, GlobalWindow>() {
            @Override
            public void process(Context context, Iterable<Map<String, Object>> elements, Collector<Map<String, Object>> out) throws Exception {
                // 将元素放入list中进行排序,
                List<Map<String, Object>> list = new ArrayList<>();
                for (Map<String, Object> element : elements) {
                    list.add(element);
                }
                list.sort((o1, o2) -> (int) (Long.parseLong(o1.get("msgTime").toString()) - Long.parseLong(o2.get("msgTime").toString())));
                // 将排好序的数据写出
                list.forEach(data -> out.collect(data));
            }
        }).setParallelism(1);

        SingleOutputStreamOperator<String> jsonStream = resStream.map(data -> new JSONObject(data).toJSONString());


        //jsonStream.print();


        // 写入文件
        //jsonStream.writeAsText("C:\\Users\\13099\\Desktop\\1.txt").setParallelism(1);

        // 不指定分区去写入kafka
        jsonStream.addSink(new FlinkKafkaProducer010<>("kafka35:9092", "data", new SimpleStringSchema())).setParallelism(1);


        // 根据vin码Hash去获得分区
      /*  jsonStream.addSink(new FlinkKafkaProducer010<>("data", new SimpleStringSchema(), props, new FlinkKafkaPartitioner<String>() {
            @Override
            public int partition(String s, byte[] bytes, byte[] bytes1, String s2, int[] ints) {
                int partition = -1;
                try {
                    byte[] digest = MessageDigest.getInstance("md5").digest(JSON.parseObject(s).getString("vin").getBytes());
                    int digestNum = CommonUtil.byteArrayToInt(digest);
                    if (digestNum < 0) {
                        digestNum = -digestNum;
                    }
                    partition = digestNum % ints.length;

                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                return partition;
            }
        })).setParallelism(1);*/

        env.execute("SortFile2Kafka");
    }
}

/**
 * 自定义excel source
 */
class ExcelSource extends RichSourceFunction<Map<String, Object>> {

    private String filePath;
    private String[] attName;
    private String[] dataName;

    public ExcelSource(String filePath, String[] attName, String[] dataName) {
        this.filePath = filePath;
        this.attName = attName;
        this.dataName = dataName;
    }

    /**
     * 递归解析文件
     *
     * @param file
     * @param ctx
     */
    public void parse(File file, SourceContext<Map<String, Object>> ctx) throws Exception {
        if (file.isDirectory()) {
            File[] fs = file.listFiles();
            for (File f : fs) {
                parse(f, ctx);
            }
        } else {
            List<Map<String, Object>> res = FileParse.readExcle(file, attName, dataName);
            res.forEach(ele -> ctx.collect(ele));
        }
    }

    @Override
    public void run(SourceContext<Map<String, Object>> ctx) throws Exception {
        parse(new File(filePath), ctx);
    }

    @Override
    public void cancel() {

    }
}

