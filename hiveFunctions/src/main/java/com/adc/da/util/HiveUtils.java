package com.adc.da.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HiveUtils {

    // 将List转换成array
    public static double[] listToArray(List<Double> list) {

        double arr[] = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = list.get(i).doubleValue();
        }
        return arr;
    }

    /**
     * 将字符串解析成二维的double数组
     *
     * @param cellVol
     * @return
     */
    public static double[][] parseVol(ArrayList<String> cellVol) {

        int cellNum = cellVol.get(0).substring(1, cellVol.get(0).length() - 1).split(",").length;
        double[][] vols = new double[cellVol.size()][cellNum];

        // 将字符串解析成数组
        for (int i = 0; i < cellVol.size(); i++) {
            String[] strs = cellVol.get(i).substring(1, cellVol.get(i).length() - 1).split(",");
            double[] voltage = new double[strs.length];
            for (int i1 = 0; i1 < strs.length; i1++) {
                voltage[i] = Double.parseDouble(strs[i]);
            }
            vols[i] = voltage;
        }
        return vols;
    }
}
