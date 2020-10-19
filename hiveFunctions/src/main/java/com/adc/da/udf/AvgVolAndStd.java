package com.adc.da.udf;

import com.adc.da.util.HiveUtils;
import com.adc.da.util.MatlabUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;


/**
 * 单体离散度高模型算法，计算电压的偏差均值和偏差方差
 *
 * hive函数：vol_avg_std()
 */
public class AvgVolAndStd extends UDF {

    /**
     *
     * @param cellVols
     * @param flag   std:偏差方差  avg：偏差均值
     * @return
     */

    public ArrayList<Double> evaluate(ArrayList<String> cellVols, String flag) {

        double[][] batteryCellVoltage = HiveUtils.parseVol(cellVols);

        /* 行列转换->求平均值->行列转换*/
        double[][] meanValue = MatlabUtil.unpiovt(MatlabUtil.mean(MatlabUtil.unpiovt(batteryCellVoltage), 1));
        /* 减去平均获取差值*/
        double[][] Vdet = MatlabUtil.sub(batteryCellVoltage, meanValue);
        /* 获取数据的行数*/
        int columnLenght = batteryCellVoltage[0].length;
        double[][] x = new double[1][columnLenght];
        double[][] y = new double[1][columnLenght];
        for (int column = 0; column < columnLenght; column++) {
            x[0][column] = MatlabUtil.abs(MatlabUtil.mean(MatlabUtil.column(Vdet, column), 1))[0][0];
            y[0][column] = MatlabUtil.abs(MatlabUtil.std(MatlabUtil.column(Vdet, column), 0, 1))[0][0];
        }
        ArrayList<Double> res = new ArrayList<>();

        if (flag.equals("avg")) {
            for (double v : x[0]) {
                res.add(new Double(v));
            }
        } else if (flag.equals("std")) {
            for (double v : y[0]) {
                res.add(new Double(v));
            }
        } else
            return null;
        return res;

    }
}
