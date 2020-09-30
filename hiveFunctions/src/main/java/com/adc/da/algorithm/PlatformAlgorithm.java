package com.adc.da.algorithm;

import com.adc.da.util.MatlabUtil;

import java.text.ParseException;

public class PlatformAlgorithm {

    /**
     * 模型一：单体电压离散度高预警
     * @param batteryCellVoltage   所有单体电压-二维数组
     * @param th1
     * @param th2
     * @return
     */
    public int highDisPersionEarlyWarning(double[][] batteryCellVoltage, int th1, int th2){

        /* 行列转换->求平均值->行列转换*/
        double[][] meanValue = MatlabUtil.unpiovt(MatlabUtil.mean(MatlabUtil.unpiovt(batteryCellVoltage), 1));
        /* 减去平均获取差值*/
        double[][] Vdet = MatlabUtil.sub(batteryCellVoltage, meanValue);
        /* 获取数据的行数*/
        int columnLenght = batteryCellVoltage[0].length;
        double[][] x = new double[1][columnLenght];
        double[][] y = new double[1][columnLenght];
        for (int column=0; column<columnLenght; column++){
            x[0][column] = MatlabUtil.abs(MatlabUtil.mean(MatlabUtil.column(Vdet, column), 1))[0][0];
            y[0][column] = MatlabUtil.abs(MatlabUtil.std(MatlabUtil.column(Vdet,column),0, 1))[0][0];
        }
        double[][] xidx;
        double[][] yidx;
        xidx = MatlabUtil.boxoutlier(x, th1);
        yidx = MatlabUtil.boxoutlier(x, th2);
        if (!MatlabUtil.isEmpty(xidx) || !MatlabUtil.isEmpty(yidx)){
            return 1;  //预警
        } else {
            return 0;  //不预警
        }

    }

    /**
     * 模型二：
     * @param max_v
     * @param min_v
     * @param th
     * @return
     */
    public int vJumpConsist(double[][] max_v, double[][] min_v, double th){

        /* 求差分->求绝对值->求和*/
        double[][] xx = MatlabUtil.sum(MatlabUtil.abs(MatlabUtil.diff(max_v)));   /* 最高电压 曲线斜率 绝对值 和*/
        double[][] yy = MatlabUtil.sum(MatlabUtil.abs(MatlabUtil.diff(min_v)));   /* 最低电压 曲线斜率 绝对值 和*/
        double[][] zz = MatlabUtil.squareBrackets(MatlabUtil.abs(MatlabUtil.diff(max_v)), MatlabUtil.abs(MatlabUtil.diff(min_v)));
        if (MatlabUtil.max(MatlabUtil.squareBrackets(xx, yy))[0][0] > 0 && MatlabUtil.mean(zz, 1)[0][0] > 0.005){
            if ( (MatlabUtil.min(MatlabUtil.squareBrackets(xx, yy))[0][0]/MatlabUtil.max(MatlabUtil.squareBrackets(xx, yy))[0][0]) < th){
                return 1;   /* 预警*/
            } else {
                return 0;   /* 不预警*/
            }
        } else {
            return 0;      /* 不预警*/
        }

    }

    /**
     * 模型三：绝缘突降预警
     * @param ri
     * @param th
     * @param win
     * @return
     */
    public int rReduce(double[][] ri, int th, int win){

        double[][] rSm = MatlabUtil.medianSmooth(ri,win);
        /* 求差值->行列转换*/
        double[][] rSmDet = MatlabUtil.unpiovt(MatlabUtil.diff(rSm));
        /* 调用find函数*/
        double[][] idex = MatlabUtil.reduceFind(rSmDet, -th);
        if (!MatlabUtil.isEmpty(idex)){
            return 1;   /* 预警*/
        } else {
            return 0;   /* 不预警*/
        }

    }


    /**
     * 模型四：电芯自放电大模型算法  (后边预警结果是自己写的，matlab中只给了预警的的输出)
     * @param dates   日期
     * @param odo     累计里程
     * @param soc     soc
     * @param vel     车速
     * @param cellVoltage   单体电压
     * @return
     */
    public int selfDischargeBig(String[][] dates, double[][] odo, double[][] soc, double[][] vel, double[][] cellVoltage) throws ParseException {

        double[][] dateDet =  MatlabUtil.dateDiff(dates);   /* 计算日期之间相差的秒数*/
        int[][] idx = MatlabUtil.selfDischargeBigFind(dateDet, 0.5*24*60*60);   /* 获取日期之差大于0.5天数据位置*/
        int warningFlag = 0;   /* 0:不预警   1:预警*/
        /* 循环遍历行*/
        for (int i=0; i<idx.length; i++){
            int idxi = idx[i][0];
            int idxj = idx[i][0] - 1;
            if (odo[idxi][0] == odo[idxj][0] && (soc[idxi][0]-soc[idxj][0]) >= -5 && (soc[idxi][0]-soc[idxj][0]) <= 5
                    && vel[idxi][0] == 0 && vel[idxj][0] == 0){
                double[][] x =  MatlabUtil.divide(MatlabUtil.sub(MatlabUtil.row(cellVoltage, idxj), MatlabUtil.row(cellVoltage, idxi)), MatlabUtil.dateSub(dates[idxi][0], dates[idxj][0]));
                double[][] multiplier = new double[][]{{1000}};
                x = MatlabUtil.multiplication(x, multiplier);
                if (MatlabUtil.max(x)[0][0] > (MatlabUtil.mean(x)[0][0] + 6*MatlabUtil.std(x)[0][0]) && MatlabUtil.max(x)[0][0] > 30) {                    /* 为行向量和列向量时支持*/
                    warningFlag = 1;
                    break;
                }
            }
        }
        return warningFlag;

    }

    /**
     * 模型七：充电压差扩大模型算法
     * @param voltageDifference  电压差：mV为单位（10个电压值）
     * @param time      相差天数
     * @return
     */
    public int chargeDifferentialVoltageExpansion(double[] voltageDifference, double[] time){

        double[] y = voltageDifference;            /* 压差：线形图Y坐标*/
        double[] ab = MatlabUtil.linearRegression(time, y);   /* y=ax+b 返回a、b的值*/
        if (ab[0] > 0.05 && (MatlabUtil.max(time) - MatlabUtil.min(time)) > 40){   /* 直线斜率大于0.05，且最高压差与最低压差之间差异大于40mV.*/
            return 1;   /* 预警*/
        } else {
            return 0;   /* 不预警*/
        }

    }


    /**
     * 模型七：充电压差扩大模型算法
     * @param voltageDifference  电压差：mV为单位（10个电压值）
     * @param time     字符串类型
     * @return
     */
    public int chargeDifferentialVoltageExpansion(double[] voltageDifference, String[] time){

        double[] x = MatlabUtil.dateDiff(time);    /* 日期相差天数：线形图X坐标*/
        double[] y = voltageDifference;            /* 压差：线形图Y坐标*/
        double[] ab = MatlabUtil.linearRegression(x, y);   /* y=ax+b 返回a、b的值*/
        if (ab[0] > 0.05 && (MatlabUtil.max(x) - MatlabUtil.min(x)) > 40){   /* 直线斜率大于0.05，且最高压差与最低压差之间差异大于40mV.*/
            return 1;   /* 预警*/
        } else {
            return 0;   /* 不预警*/
        }

    }


    /**
     * 模型七：拟合直线x轴、y轴交点
     * @param voltageDifference  电压差：mV为单位（10个电压值）
     * @param time     字符串类型
     * @return
     */
    public double[] linearRegression(double[] voltageDifference, String[] time){

        double[] x = MatlabUtil.dateDiff(time);    /* 日期相差天数：线形图X坐标*/
        double[] y = voltageDifference;            /* 压差：线形图Y坐标*/
        double[] ab = MatlabUtil.linearRegression(x, y);   /* y=ax+b 返回a、b的值*/
        double xValue = -(ab[1]/ab[0]);
        double yValue = ab[1];
        double[] result = new double[]{xValue, yValue};
        return result;

    }



}
