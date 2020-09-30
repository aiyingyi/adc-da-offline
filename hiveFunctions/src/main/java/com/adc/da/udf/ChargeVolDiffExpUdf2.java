package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

public class ChargeVolDiffExpUdf2 extends UDF {

    public String evaluate(ArrayList<Double> volDiff, ArrayList<Double> dayDiff) {

        if (volDiff == null || dayDiff == null) {
            return null;
        }


        return new PlatformAlgorithm().chargeDifferentialVoltageExpansion(listToArray(volDiff), listToArray(dayDiff)) + "";
    }

    public double[] listToArray(ArrayList<Double> list) {

        double arr[] = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = (double) list.get(i);
        }
        return arr;
    }

}
