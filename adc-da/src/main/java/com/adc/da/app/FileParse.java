package com.adc.da.app;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import sun.security.provider.MD5;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FileParse {

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
     * 计算一个文件的行数
     *
     * @param file
     * @return
     */
    public static long getFileRecordNum(File file) {

        if (file.isDirectory()) {
            return -1;
        }
        XSSFWorkbook xssfWorkbook = null;
        try {
            xssfWorkbook = new XSSFWorkbook(file);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidFormatException e) {
            e.printStackTrace();
        }
        //读取第一个工作表
        XSSFSheet sheet = xssfWorkbook.getSheetAt(0);
        //获取最后一行的num，即总行数。此处从0开始计数
        int maxRow = sheet.getLastRowNum();
        return maxRow;
    }

    /**
     * 计算目录中的所有文件记录个数
     *
     * @param file
     * @return
     */
    public static long getRecordSize(File file) {
        long size = 0;
        if (!file.isDirectory()) {
            size = getFileRecordNum(file);
        } else {
            for (File f : file.listFiles()) {
                size = size + getFileRecordNum(f);
            }
        }
        return size;
    }

    /**
     * 解析单个excel文件
     *
     * @param file          文件名
     * @param attributeName excel中的字段名称
     * @param dataName      json中的字段名称
     * @throws Exception
     */
    public static List<Map<String, String>> readExcle(File file, String[] attributeName, String[] dataName) throws Exception {

        List<Map<String, String>> list = new ArrayList<>();
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
                Map<String, String> map = new HashMap<>();
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

    public static void main2(String[] args) throws ParseException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.11.35:9092");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    }

    public static void main(String[] args) {
        System.out.println(getRecordSize(new File("C:\\Users\\13099\\Desktop\\2")));
    }
}

