package com.adc.da;

import com.adc.da.algorithm.PlatformAlgorithm;
import com.adc.da.util.MatlabUtil;

import java.text.ParseException;

public class Test {
    public static void main(String[] args) throws ParseException {
        System.out.println(new PlatformAlgorithm().chargeDifferentialVoltageExpansion(
                new double[]{2.0, 1.0, 5.0, 6.0, 2.0, 8.0, 52.0},
                new String[]{"2020-09-14 12:14:14", "2020-09-14 12:14:14", "2020-09-14 12:14:14", "2020-09-14 12:14:14", "2020-09-14 12:14:14", "2020-09-14 12:14:14", "2020-09-14 12:14:14"}));

        //System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse());

        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //System.out.println(sdf.parse(sdf.format(new Date(Long.parseLong("1601444962")))));

        //for (double v : MatlabUtil.dateDiff(new String[]{"2020-09-14 12:14:14","2020-09-15 12:14:14","2020-09-16 12:14:14"})) {
        //    System.out.println(v);
        //}

    }

}
