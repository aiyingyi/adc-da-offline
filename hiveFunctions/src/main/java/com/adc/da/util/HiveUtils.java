package com.adc.da.util;

import java.util.ArrayList;

public class HiveUtils {

    // 将List转换成array
    public static double[] listToArray(ArrayList<Double> list) {

        double arr[] = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] =  list.get(i);
        }
        return arr;
    }
}
