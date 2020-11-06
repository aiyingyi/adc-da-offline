package com.adc.da.app;


import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
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
    public static String dateToStamp(String s)  {
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
     * @param fileName      文件名
     * @param attributeName excel中的字段名称
     * @param dataName      json中的字段名称
     * @throws Exception
     */
    public static List<Map<String, String>> readExcle(String fileName, String[] attributeName, String[] dataName) throws Exception {

        List<Map<String, String>> list = new ArrayList<>();
        try {
            //创建工作簿
            XSSFWorkbook xssfWorkbook = new XSSFWorkbook(new FileInputStream(fileName));
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

    public static void main(String[] args) throws ParseException {

        String[] attName = new String[]{"VIN", "报文时间", "车速", "车辆状态", "运行模式", "累计里程", "档位", "充电状态",
                "加速踏板行程值", "制动踏板状态", "总电压", "总电流", "SOC", "绝缘电阻", "定位状态", "纬度", "经度",
                "通用报警标志", "单体电池总数", "可充电储能温度探针个数", "单体电池电压", "可充电储能装置温度数据"};

        String[] dataName = new String[]{"vin", "msgTime", "speed", "startupStatus", "runMode", "odo", "gearStatus", "chargeStatus", "aptv", "bptv",
                "totalVoltage", "totalCurrent", "soc", "insulationResistance", "positionStatus", "longitude", "latitude", "failure", "cellNum", "probeNum",
                "cellVoltage", "probeTemperature"};
        try {
            List<Map<String, String>> list = readExcle("E:\\软件安装\\原始数据\\数据\\[LGJE13EA8HM612678]_2018-04-15_00-00-00_2018-04-15_23-59-59.xlsx", attName, dataName);

            List<Map<String, String>> res = list.stream().map(new Function<Map<String, String>, Map<String, String>>() {
                @Override
                public Map<String, String> apply(Map<String, String> data) {

                    data.put("msgTime", dateToStamp(data.get("msgTime")));
                    //

                    if("启动".equals(data.get("startupStatus"))){
                        data.put("startupStatus","1");
                    }else{
                        data.put("startupStatus","0");
                    }


                    if("未充电状态".equals(data.get("chargeStatus"))){
                        data.put("startupStatus","1");
                    }else{
                        data.put("startupStatus","0");
                    }




                    return data;
                }
            }).collect(Collectors.toList());


            res.forEach(data-> System.out.println(data));

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

