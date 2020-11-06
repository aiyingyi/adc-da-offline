package com.adc.da.app;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileParse {


    public static void readExcle(String fileName) throws Exception {

        try {

            //创建工作簿
            XSSFWorkbook xssfWorkbook = new XSSFWorkbook(new FileInputStream(fileName));

            //读取第一个工作表
            XSSFSheet sheet = xssfWorkbook.getSheetAt(0);
            //获取最后一行的num，即总行数。此处从0开始计数
            int maxRow = sheet.getLastRowNum();
            System.out.println("总行数为：" + maxRow);
            for (int row = 0; row <= maxRow; row++) {
                //获取最后单元格num，即总单元格数 ***注意：此处从1开始计数***
                int maxRol = sheet.getRow(row).getLastCellNum();
                System.out.println("--------第" + row + "行的数据如下--------");
                for (int rol = 0; rol < maxRol; rol++) {
                    System.out.print(sheet.getRow(row).getCell(rol) + "  ");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

        try {
            readExcle("E:\\软件安装\\原始数据\\数据\\[LGJE13EA8HM612678]_2018-04-15_00-00-00_2018-04-15_23-59-59.xlsx");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
