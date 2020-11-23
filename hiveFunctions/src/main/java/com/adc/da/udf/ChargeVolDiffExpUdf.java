package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import com.adc.da.util.HiveUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * 充电压差扩大模型算法自定义UDF函数
 * hive函数名：charge_vol_diff_exp()
 *
 * @author aiyingyi
 * @date 2020/10/09
 */
public class ChargeVolDiffExpUdf extends UDF {
    /**
     * 传入的列是数组类型，使用List去接收，而不是数组
     *
     * @param volDiff 充电压差数组
     * @param time    充电时间
     * @return 0/1 表示是否发生预警
     */
    public String evaluate(ArrayList<Double> volDiff, ArrayList<String> time, double th1, double th2) {
        if (volDiff == null || time == null) {
            return null;
        }
        String[] timeStr = new String[time.size()];
        for (int i = 0; i < time.size(); i++) {
            timeStr[i] = (String) time.get(i);
        }
        return new PlatformAlgorithm().chargeDifferentialVoltageExpansion(HiveUtils.listToArray(volDiff), timeStr, th1,th2) + "";
    }

    public static void main(String[] args) {

        double[] vol = {1.2,2.2};
        String[] time = {"2018-02-01 12:14:14","2018-02-01 13:14:14"};
        ArrayList<String> timeList = new ArrayList<>();
        ArrayList<Double> volList = new ArrayList<>();
        for (double v : vol) {
            volList.add(v);
        }
        for (String s : time) {
            timeList.add(s);
        }
        System.out.println(new ChargeVolDiffExpUdf().evaluate(volList, timeList, 0.5, 1.0));
    }
}
