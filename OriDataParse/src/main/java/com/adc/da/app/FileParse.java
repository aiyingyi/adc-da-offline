package com.adc.da.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.Put;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import javax.xml.transform.Source;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FileParse {
    public static double[] str2DouleArr(String str) {
        if (str == null) {
            return null;
        }
        str = str.substring(1, str.length() - 1);
        String[] split = str.split(",");

        double[] res = new double[split.length];
        for (int i = 0; i < split.length; i++) {
            res[i] = Double.parseDouble(split[i]);
        }
        return res;
    }


    /**
     * 将日期转换成时间戳
     *
     * @param s
     * @return
     * @throws ParseException
     */
    public static String dateToStamp(String s) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long ts = date.getTime();
        return String.valueOf(ts);
    }

    /**
     * @param file          文件名
     * @param attributeName excel中的字段名称
     * @param dataName      json中的字段名称
     * @throws Exception
     */
    public static List<Map<String, Object>> readExcle(File file, String[] attributeName, String[] dataName) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();

        try {
            //创建工作簿
            XSSFWorkbook xssfWorkbook = new XSSFWorkbook(file);
            //读取第一个工作表
            XSSFSheet sheet = xssfWorkbook.getSheetAt(0);
            int[] location = new int[attributeName.length];
            for (int i = 0; i < location.length; i++) {
                location[i] = -1;
            }
            //获取最后一行的num，即总行数。此处从0开始计数
            int maxRow = sheet.getLastRowNum();
            XSSFRow header = sheet.getRow(0);
            int cellNum = header.getLastCellNum();
            // 寻找字段在excel中的位置
            for (int i = 0; i < attributeName.length; i++) {
                for (int j = 0; j < cellNum; j++) {
                    if (header.getCell(j).toString().trim().equals(attributeName[i])) {
                        location[i] = j;
                        break;
                    }
                }
            }
            for (int row = 1; row <= maxRow; row++) {
                XSSFRow sheetRow = sheet.getRow(row);
                Map<String, Object> map = new HashMap<>();
                for (int att = 0; att < attributeName.length; att++) {
                    map.put(dataName[att], sheetRow.getCell(location[att]).toString());
                }
                list.add(map);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }


    // 对读取的原始数据进行解析
    public static List<Map<String, Object>> parseVehicleData(File file) {
        String[] attName = new String[]{"VIN", "报文时间", "车速", "车辆状态", "运行模式", "累计里程", "档位", "充电状态",
                "加速踏板行程值", "制动踏板状态", "总电压", "总电流", "SOC", "绝缘电阻", "定位状态", "纬度", "经度",
                "通用报警标志", "单体电池总数", "可充电储能温度探针个数", "单体电池电压", "可充电储能装置温度数据"};
        String[] dataName = new String[]{"vin", "msgTime", "speed", "startupStatus", "runMode", "odo", "gearStatus", "chargeStatus", "aptv", "bptv",
                "totalVoltage", "totalCurrent", "soc", "insulationResistance", "positionStatus", "longitude", "latitude", "failure", "cellNum", "probeNum",
                "cellVoltage", "probeTemperature"};
        try {
            List<Map<String, Object>> list = readExcle(file, attName, dataName);
            List<Map<String, Object>> res = list.stream().map(new Function<Map<String, Object>, Map<String, Object>>() {
                @Override
                public Map<String, Object> apply(Map<String, Object> data) {
                    data.put("msgTime", dateToStamp(data.get("msgTime").toString()));
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

                    double[] probeTemperature = str2DouleArr(data.get("probeTemperature").toString());
                    double[] cellVoltage = str2DouleArr(data.get("cellVoltage").toString());

                    data.put("probeTemperature", probeTemperature);
                    data.put("cellVoltage", cellVoltage);

                    return data;

                }
            }).collect(Collectors.toList());
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void sendToKafak(File file, KafkaProducer<String, String> producer) {

        File[] fs = file.listFiles();
        for (File f : fs) {
            if (f.isDirectory())    //若是目录，则递归打印该目录下的文件
                sendToKafak(f, producer);
            if (f.isFile()) {
                List<Map<String, Object>> res = parseVehicleData(f);


                res.forEach(record -> {
                    producer.send(new ProducerRecord<String, String>("data", "0001", new JSONObject(record).toJSONString()));
                });
            }
        }
    }

    public static void main(String[] args) throws ParseException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.11.35:9092");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //sendToKafak(new File("E:\\软件安装\\原始数据\\数据"), producer);
        sendToKafak(new File("C:\\Users\\13099\\Desktop\\2"), producer);
        producer.close();


    }
}

