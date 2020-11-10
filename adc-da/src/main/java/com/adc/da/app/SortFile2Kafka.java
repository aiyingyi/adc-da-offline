package com.adc.da.app;


import com.adc.da.util.ComUtil;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * flink 批处理，用于文件合并排序并发送给kafka
 */
public class SortFile2Kafka {
    public static void main(String[] args) throws Exception {

        // 获取文件路径
        String sourcePath = "C:\\Users\\13099\\Desktop\\1\\11.xlsx";//args[0];

        // Todo 定义源文件和平台数据映射字段值    如何在用户配置后获取数据
        String[] attName = new String[]{"VIN", "报文时间", "车速", "车辆状态", "运行模式", "累计里程", "档位", "充电状态",
                "加速踏板行程值", "制动踏板状态", "总电压", "总电流", "SOC", "绝缘电阻", "定位状态", "纬度", "经度",
                "通用报警标志", "单体电池总数", "可充电储能温度探针个数", "单体电池电压", "可充电储能装置温度数据"};
        String[] dataName = new String[]{"vin", "msgTime", "speed", "startupStatus", "runMode", "odo", "gearStatus", "chargeStatus", "aptv", "bptv",
                "totalVoltage", "totalCurrent", "soc", "insulationResistance", "positionStatus", "longitude", "latitude", "failure", "cellNum", "probeNum",
                "cellVoltage", "probeTemperature"};

        // 获取数据的条数，用来开启一个计数窗口
        long recordSize = FileParse.getRecordSize(new File(sourcePath));
        StreamExecutionEnvironment env = ComUtil.initEnvironment();
        env.setParallelism(1);
        // 从数据源获取数据
        SingleOutputStreamOperator<Map<String, String>> dataSource = env.addSource(new TestExcelSource(sourcePath, attName, dataName));

        SingleOutputStreamOperator<Object> resStream = dataSource.map(new MapFunction<Map<String, String>, Map<String, String>>() {
            @Override
            public Map<String, String> map(Map<String, String> data) throws Exception {
                // 将时间转换成时间戳
                data.put("msgTime", FileParse.dateToStamp(data.get("msgTime")));
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
                return data;
            }
        }).countWindowAll(recordSize).process(new ProcessAllWindowFunction<Map<String, String>, Object, GlobalWindow>() {
            @Override
            public void process(Context context, Iterable<Map<String, String>> elements, Collector<Object> out) throws Exception {
                // 将元素放入list中进行排序,
                List<Map<String, String>> list = new ArrayList<>();
                for (Map<String, String> element : elements) {
                    list.add(element);
                }
                list.sort((o1, o2) -> (int) (Long.parseLong(o1.get("msgTime")) - Long.parseLong(o2.get("msgTime"))));
                // 将排好序的数据写出
                list.forEach(data -> out.collect(data));
            }
        }).setParallelism(1);
        //resStream.map(data -> Json.toString(data)).writeAsText("C:\\Users\\13099\\Desktop\\1.txt");
        resStream.map(data -> Json.toString(data)).print();

        env.execute("");
    }
}

/**
 * 自定义excel source
 */
class TestExcelSource extends RichSourceFunction<Map<String, String>> {

    private String filePath;
    private String[] attName;
    private String[] dataName;

    public TestExcelSource(String filePath, String[] attName, String[] dataName) {
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
    public void parse(File file, SourceContext<Map<String, String>> ctx) throws Exception {
        if (file.isDirectory()) {
            File[] fs = file.listFiles();
            for (File f : fs) {
                parse(f, ctx);
            }
        } else {
            List<Map<String, String>> res = FileParse.readExcle(file, attName, dataName);
            res.forEach(ele -> ctx.collect(ele));
        }
    }

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        parse(new File(filePath), ctx);
    }

    @Override
    public void cancel() {

    }
}

