package com.adc.da.algorithm;

import com.adc.da.util.MathUtil;
import com.adc.da.util.MatlabUtil;


import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

public class PlatformAlgorithm {

    /**
     * 模型一：单体电压离散度高预警
     *
     * @param batteryCellVoltage 所有单体电压-二维数组
     * @param th1
     * @param th2
     * @return
     */
    public int highDisPersionEarlyWarning(double[][] batteryCellVoltage, int th1, int th2) {

        /* 行列转换->求平均值->行列转换            每行求一个平均值（每行代表一组电压值）*/
        double[][] meanValue = MatlabUtil.unpiovt(MatlabUtil.mean(MatlabUtil.unpiovt(batteryCellVoltage), 1));
        /* 减去平均获取差值            每行电压值减去每行的平均值*/
        double[][] Vdet = MatlabUtil.sub(batteryCellVoltage, meanValue);
        /* 获取数据的列数*/
        int columnLength = batteryCellVoltage[0].length;
        double[][] x = new double[1][columnLength];
        double[][] y = new double[1][columnLength];
        for (int column = 0; column < columnLength; column++) {
            x[0][column] = MatlabUtil.abs(MatlabUtil.mean(MatlabUtil.column(Vdet, column), 1))[0][0];
            y[0][column] = MatlabUtil.abs(MatlabUtil.std(MatlabUtil.column(Vdet, column), 0, 1))[0][0];
        }
        double[][] xidx;
        double[][] yidx;
        xidx = MatlabUtil.boxoutlier(x, th1);
        yidx = MatlabUtil.boxoutlier(x, th2);
        if (!MatlabUtil.isEmpty(xidx) || !MatlabUtil.isEmpty(yidx)) {
            return 1;  //预警
        } else {
            return 0;  //不预警
        }

    }


    /**
     * 模型一：单体电压离散度高预警  返回预警的电池单体编号
     *
     * @param batteryCellVoltage 所有单体电压-二维数组
     * @param th1
     * @param th2
     * @return
     */
    public int[] highDisPersionEarlyWarningCellNumber(double[][] batteryCellVoltage, int th1, int th2) {

        /* 行列转换->求平均值->行列转换            每行求一个平均值（每行代表一组电压值）*/
        double[][] meanValue = MatlabUtil.unpiovt(MatlabUtil.mean(MatlabUtil.unpiovt(batteryCellVoltage), 1));
        /* 减去平均获取差值            每行电压值减去每行的平均值*/
        double[][] Vdet = MatlabUtil.sub(batteryCellVoltage, meanValue);
        /* 获取数据的列数*/
        int columnLength = batteryCellVoltage[0].length;
        double[][] x = new double[1][columnLength];
        double[][] y = new double[1][columnLength];
        for (int column = 0; column < columnLength; column++) {
            x[0][column] = MatlabUtil.abs(MatlabUtil.mean(MatlabUtil.column(Vdet, column), 1))[0][0];
            y[0][column] = MatlabUtil.abs(MatlabUtil.std(MatlabUtil.column(Vdet, column), 0, 1))[0][0];
        }
        double[][] xidx;
        double[][] yidx;
        xidx = MatlabUtil.boxoutlier(x, th1);
        yidx = MatlabUtil.boxoutlier(x, th2);
        if (!MatlabUtil.isEmpty(xidx) || !MatlabUtil.isEmpty(yidx)) {
            int xidxRowLength = xidx.length;
            int yidxRowLength = yidx.length;
            int[] result = new int[xidxRowLength + yidxRowLength];
            for (int i = 0; i < xidxRowLength; i++) {
                result[i] = (int) xidx[i][0];
            }
            for (int j = 0; j < yidxRowLength; j++) {
                result[xidxRowLength + j] = (int) yidx[j][0];
            }
            return result;
        } else {
            return null;
        }


    }


    /**
     * 模型二：单体电压波动性差异大模型算法（二维数据参数）
     *
     * @param max_v
     * @param min_v
     * @param th
     * @return
     */
    public int vJumpConsist(double[][] max_v, double[][] min_v, double th) {

        /* 求差分->求绝对值->求和*/
        double[][] xx = MatlabUtil.sum(MatlabUtil.abs(MatlabUtil.diff(max_v)));   /* 最高电压 曲线斜率 绝对值 和*/
        double[][] yy = MatlabUtil.sum(MatlabUtil.abs(MatlabUtil.diff(min_v)));   /* 最低电压 曲线斜率 绝对值 和*/
        double[][] zz = MatlabUtil.squareBrackets(MatlabUtil.abs(MatlabUtil.diff(max_v)), MatlabUtil.abs(MatlabUtil.diff(min_v)));
        if (MatlabUtil.max(MatlabUtil.squareBrackets(xx, yy))[0][0] > 0 && MatlabUtil.mean(zz, 1)[0][0] > 0.005) {
            if ((MatlabUtil.min(MatlabUtil.squareBrackets(xx, yy))[0][0] / MatlabUtil.max(MatlabUtil.squareBrackets(xx, yy))[0][0]) < th) {
                return 1;   /* 预警*/
            } else {
                return 0;   /* 不预警*/
            }
        } else {
            return 0;      /* 不预警*/
        }

    }


    /**
     * 模型二：单体电压波动性差异大模型算法（一维数据参数）
     *
     * @param max_v
     * @param min_v
     * @param th
     * @return
     */
    public int vJumpConsist(double[] max_v, double[] min_v, double th) {

        /* 求差分->求绝对值->求和*/
        double xx = MatlabUtil.sum(MatlabUtil.abs(MatlabUtil.diff(max_v)));   /* 最高电压 曲线斜率 绝对值 和*/
        double yy = MatlabUtil.sum(MatlabUtil.abs(MatlabUtil.diff(min_v)));   /* 最低电压 曲线斜率 绝对值 和*/
        double[] zz = MatlabUtil.squareBrackets(MatlabUtil.abs(MatlabUtil.diff(max_v)), MatlabUtil.abs(MatlabUtil.diff(min_v)));
        if ((xx > yy ? xx : yy) > 0 && MatlabUtil.mean(zz) > 0.005) {
            if (MathUtil.divideDouble((xx < yy ? xx : yy), (xx > yy ? xx : yy)) < th) {
                return 1;   /* 预警*/
            } else {
                return 0;   /* 不预警*/
            }
        } else {
            return 0;      /* 不预警*/
        }

    }


    /**
     * 模型三：一维数据-绝缘突降预警
     *
     * @param ri
     * @param th
     * @param win
     * @return
     */
    public int rReduce(double[] ri, int th, int win) {

        double[] rSm = MatlabUtil.medianSmooth(ri, win);
        /* 求差值*/
        double[] rSmDet = MatlabUtil.diff(rSm);
        /* 调用find函数*/
        double[] idex = MatlabUtil.reduceFind(rSmDet, -th);
        if (idex == null || idex.length == 0) {
            return 0;   /* 不预警*/
        } else {
            return 1;   /* 预警*/
        }

    }

    /**
     * 模型三：二维数据-绝缘突降预警
     *
     * @param ri
     * @param th
     * @param win
     * @return
     */
    public int rReduce(double[][] ri, int th, int win) {

        double[][] rSm = MatlabUtil.medianSmooth(ri, win);
        /* 求差值->行列转换*/
        double[][] rSmDet = MatlabUtil.unpiovt(MatlabUtil.diff(rSm));
        /* 调用find函数*/
        double[][] idex = MatlabUtil.reduceFind(rSmDet, -th);
        if (!MatlabUtil.isEmpty(idex)) {
            return 1;   /* 预警*/
        } else {
            return 0;   /* 不预警*/
        }

    }


    /**
     * 模型四：电芯自放电大模型算法  (后边预警结果是自己写的，matlab中只给了预警的的输出)  ：二维数据
     *
     * @param dates       日期
     * @param odo         累计里程
     * @param soc         soc
     * @param vel         车速
     * @param cellVoltage 单体电压
     * @return
     */
    public int selfDischargeBig(String[][] dates, double[][] odo, double[][] soc, double[][] vel, double[][] cellVoltage) throws ParseException {

        double[][] dateDet = MatlabUtil.dateDiff(dates);   /* 计算日期之间相差的秒数*/
        int[][] idx = MatlabUtil.selfDischargeBigFind(dateDet, 0.5 * 24 * 60 * 60);   /* 获取日期之差大于0.5天数据位置*/
        int warningFlag = 0;   /* 0:不预警   1:预警*/
        /* 循环遍历行*/
        for (int i = 0; i < idx.length; i++) {
            int idxi = idx[i][0];
            int idxj = idx[i][0] - 1;
            if (odo[idxi][0] == odo[idxj][0] && (soc[idxi][0] - soc[idxj][0]) >= -5 && (soc[idxi][0] - soc[idxj][0]) <= 5
                    && vel[idxi][0] == 0 && vel[idxj][0] == 0) {
                double[][] x = MatlabUtil.divide(MatlabUtil.sub(MatlabUtil.row(cellVoltage, idxj), MatlabUtil.row(cellVoltage, idxi)), MatlabUtil.dateSub(dates[idxi][0], dates[idxj][0]));
                double[][] multiplier = new double[][]{{1000}};
                x = MatlabUtil.multiplication(x, multiplier);
                if (MatlabUtil.max(x)[0][0] > (MatlabUtil.mean(x)[0][0] + 6 * MatlabUtil.std(x)[0][0]) && MatlabUtil.max(x)[0][0] > 30) {                    /* 为行向量和列向量时支持*/
                    warningFlag = 1;
                    break;
                }
            }
        }
        return warningFlag;

    }


    /**
     * 模型四：电芯自放电大模型算法  (后边预警结果是自己写的，matlab中只给了预警的的输出) :一维数据
     *
     * @param dates       日期
     * @param odo         累计里程
     * @param soc         soc
     * @param vel         车速
     * @param cellVoltage 单体电压
     * @return
     */
    public int selfDischargeBig(String[] dates, double[] odo, double[] soc, double[] vel, double[][] cellVoltage) throws ParseException {

        double[] dateDet = MatlabUtil.dateDiff(dates);   /* 计算日期之间相差的天数*/
        int[] idx = MatlabUtil.selfDischargeBigFind(dateDet, 0.5);   /* 获取日期之差大于0.5天数据位置*/
        int warningFlag = 0;   /* 0:不预警   1:预警*/
        /* 循环遍历行*/
        for (int i = 0; i < idx.length; i++) {
            int idxi = idx[i];
            int idxj = idx[i] - 1;
            if (odo[idxi] == odo[idxj] && (soc[idxi] - soc[idxj]) >= -5 && (soc[idxi] - soc[idxj]) <= 5
                    && vel[idxi] == 0 && vel[idxj] == 0) {
                double[] x = MatlabUtil.divide(MatlabUtil.sub(MatlabUtil.singleRow(cellVoltage, idxj), MatlabUtil.singleRow(cellVoltage, idxi)), MatlabUtil.singleDateSub(dates[idxi], dates[idxj]));
                double multiplier = 1000;
                x = MatlabUtil.multiplication(x, multiplier);
                if (MatlabUtil.max(x) > (MathUtil.addDouble(MatlabUtil.mean(x), MathUtil.multiplyDouble(6, MatlabUtil.std(x)))) && MatlabUtil.max(x) > 30) {         /* 为行向量和列向量时支持*/
                    warningFlag = 1;
                    break;
                }
            }
        }
        return warningFlag;

    }

    /**
     * 模型四：电芯自放电大模型算法  (后边预警结果是自己写的，matlab中只给了预警的的输出) :一维数据
     *
     * @param dt          日期
     * @param cellVoltage 单体电压
     * @return
     */
    public int selfDischargeBig(long[] dt, double[][] cellVoltage) throws ParseException {

        int warningFlag = 0;   /* 0:不预警   1:预警*/

        double timeDiff = (dt[1] - dt[0]) / (1000 * 60 * 60 * 24.0);

        double[] x = MatlabUtil.divide(MatlabUtil.sub(MatlabUtil.singleRow(cellVoltage, 0), MatlabUtil.singleRow(cellVoltage, 1)), timeDiff);
        double multiplier = 1000;
        x = MatlabUtil.multiplication(x, multiplier);
        if (MatlabUtil.max(x) > (MathUtil.addDouble(MatlabUtil.mean(x), MathUtil.multiplyDouble(6, MatlabUtil.std(x)))) && MatlabUtil.max(x) > 30) {         /* 为行向量和列向量时支持*/
            warningFlag = 1;

        }
        return warningFlag;

    }


    /**
     * 模型七：充电压差扩大模型算法
     *
     * @param voltageDifference 电压差：mV为单位（10个电压值）
     * @param time              字符串类型
     * @param th1               斜率（判断）
     * @param th2               压差值（判断）
     * @return
     */
    public int chargeDifferentialVoltageExpansion(double[] voltageDifference, String[] time, double th1, double th2) {

        double[] x = MatlabUtil.dateDiff(time);    /* 日期相差天数：线形图X坐标*/
        double[] y = voltageDifference;            /* 压差：线形图Y坐标*/
        double[] ab = MatlabUtil.linearRegression(x, y);   /* y=ax+b 返回a、b的值*/
        if (ab[0] > th1 && (MatlabUtil.max(x) - MatlabUtil.min(x)) > th2) {   /* 直线斜率大于0.05，且最高压差与最低压差之间差异大于40mV.*/
            return 1;   /* 预警*/
        } else {
            return 0;   /* 不预警*/
        }

    }


    /**
     * 模型七：拟合直线x轴、y轴交点
     *
     * @param voltageDifference 电压差：mV为单位（10个电压值）
     * @param time              字符串类型
     * @return
     */
    public double[] linearRegression(double[] voltageDifference, String[] time) {

        double[] x = MatlabUtil.dateDiff(time);    /* 日期相差天数：线形图X坐标*/
        double[] y = voltageDifference;            /* 压差：线形图Y坐标*/
        double[] ab = MatlabUtil.linearRegression(x, y);   /* y=ax+b 返回a、b的值*/
        double xValue = -MathUtil.divideDouble(ab[1], ab[0]);
        double yValue = ab[1];
        double[] result = new double[]{xValue, yValue};
        return result;

    }


    /**
     * 模型八：连接阻抗大模型算法（电池压差与电流正相关）
     *
     * @param vdet 电压差数组()
     * @param I    电流数组（A)
     * @param soc  电量数组
     * @param rth1 阀值1
     * @param rth2 阀值2
     * @return
     */
    public int highConnectionImpedance(double[] vdet, double[] I, double[] soc, double rth1, double rth2) {

        double[] Rdet = MatlabUtil.divide(vdet, MatlabUtil.abs(I));                                  /* 单位是mΩ*/
        if (Rdet.length > 50) {
            double[] idx = MatlabUtil.reduceFind(I, -5);                    /* 获取电流小于-5A的元素的位置*/
            double rdetMean = MatlabUtil.mean(MatlabUtil.arraySpot(Rdet, idx));         /* 获取电阻平均值*/
            double ichMean = Math.abs(MatlabUtil.mean(MatlabUtil.arraySpot(I, idx)));   /* 获取电流平均值*/
            double ichMax = Math.abs(MatlabUtil.max(MatlabUtil.arraySpot(I, idx)));     /* 获取电流最大值*/
            if (ichMean <= 30) {
                if (rdetMean >= rth1) {
                    return 1;  /* 预警，慢充电压差与电流正相关*/
                } else {
                    return 0;
                }

            } else if (ichMean >= 50) {
                if (rdetMean >= rth2) {
                    return 1;  /* 预警，快充电压差与电流正相关*/
                } else {
                    return 0;
                }
            } else if (ichMean > 30 && ichMean < 50) {
                if (ichMax <= 50) {
                    if (rdetMean >= rth1) {
                        return 1;  /* 预警，慢充电压差与电流正相关*/
                    } else {
                        return 0;
                    }
                } else {
                    if (rdetMean >= rth2) {
                        return 1;  /* 预警，快充电压差与电流正相关*/
                    } else {
                        return 0;
                    }
                }
            } else {
                return 0;
            }
        } else {
            return 0;
        }

    }


    /**
     * 模型九：BMS采样异常(支持充电、放电)
     *
     * @param vdet    压差平均值
     * @param vMaxNum 最高电压单体编号
     * @param vMinNum 最低电压单体编号
     * @param rth1    阀值1
     * @param rth2    阀值2
     * @return
     */
    public int bmsSamplingAnomaly(double vdet, int[] vMaxNum, int[] vMinNum, int rth1, int rth2) {

        if (vdet > rth1) {
            double[][] vMaxTable = MatlabUtil.tabulate(vMaxNum);     /* 数组统计*/
            double[][] vMinTable = MatlabUtil.tabulate(vMinNum);     /* 数组统计*/
            if (MatlabUtil.max(MatlabUtil.indexColumn(vMaxTable, 3)) > rth2 && MatlabUtil.max(MatlabUtil.indexColumn(vMinTable, 3)) > rth2) {   /* 最高单体和最低单体编号占比最大值高于rth2*/
                if (Math.abs(MatlabUtil.maxValueIndex(MatlabUtil.indexColumn(vMaxTable, 3)) - MatlabUtil.maxValueIndex(MatlabUtil.indexColumn(vMinTable, 3))) == 1) {
                    return 1;  /* 预警*/
                } else {
                    return 0;  /* 不预警*/
                }
            } else {
                return 0;  /* 不预警*/
            }
        } else {
            return 0;  /* 不预警*/
        }

    }


    /**
     * 模型十：容量异常
     *
     * @param vCh    充电工况下所有数据的单体电压，电芯编号0-95
     * @param vDisCh 放电工况下所有数据的单体电压，电芯编号0-95
     * @param th1    阀值：计算最高/最低单体的个数
     * @param th2    阀值：充电电芯的占比
     * @param th3    阀值：放电电芯的占比
     * @return
     */
    public int capacityAbnormal(double[][] vCh, double[][] vDisCh, int th1, double th2, double th3) {

        int vChRowLength = vCh.length;
        int vDisChRwoLength = vDisCh.length;
        double[] vMaxChNum = new double[vChRowLength * th1];       /* 存放最高单体电压的电芯编号*/
        double[] vMinDisChNum = new double[vDisChRwoLength * th1];  /* 存放最低单体电压的电芯编号*/

        /* 获取指定位置的电芯编号(单体电压最高)*/
        int vMaxChNumIndex = 0;   /* 数组索引*/
        for (int i = 0; i < vChRowLength; i++) {
            double[] temporary = MatlabUtil.getIndexRow(vCh, i);
            int[] index = MatlabUtil.getIndexBySort(temporary, false);                     /* 从小到大排序返回索引*/
            int indexLength = index.length;
            for (int j = indexLength - th1; j < indexLength; j++) {
                vMaxChNum[vMaxChNumIndex] = index[j];         /* 获取电芯编号*/
                vMaxChNumIndex++;
            }
        }
        /* 获取指定位置的电芯编号（单体电压最低）*/
        int vMinDisChNumIndex = 0;
        for (int i = 0; i < vDisChRwoLength; i++) {
            double[] temporary = MatlabUtil.getIndexRow(vDisCh, i);
            int[] index = MatlabUtil.getIndexBySort(temporary, false);                     /* 从小到大排序返回索引*/
            for (int j = 0; j < th1; j++) {
                vMinDisChNum[vMinDisChNumIndex] = index[j];         /* 获取电芯编号*/
                vMinDisChNumIndex++;
            }
        }
        double[][] vMaxChTab = MatlabUtil.tabulate(vMaxChNum);          /* 电芯编号分布统计*/
        double[][] vMinDisChTab = MatlabUtil.tabulate(vMinDisChNum);    /* 电芯编号分布统计*/
        if (MatlabUtil.max(MatlabUtil.indexColumn(vMaxChTab, 3)) > th2) {
            if (MatlabUtil.max(MatlabUtil.indexColumn(vMinDisChTab, 3)) > th3) {
                if (MatlabUtil.maxValueIndex(MatlabUtil.indexColumn(vMaxChTab, 3)) == MatlabUtil.maxValueIndex(MatlabUtil.indexColumn(vMinDisChTab, 3))) {
                    return 1;  /* 预警：电芯容量异常*/
                } else {
                    return 0;  /* 不预警*/
                }
            } else {
                return 0;      /* 不预警*/
            }
        } else {
            return 0;          /* 不预警*/
        }

    }

    public static void main(String[] args) {


        double[] a = new double[]{44.0, 43.0, 42.0, 42.0, 42.0, 43.0, 43.0, 44.0, 43.0, 42.0, 43.0, 43.0, 43.0, 43.0, 44.0, 43.0, 43.0, 42.0, 43.0, 43.0, 42.0, 43.0, 42.0, 42.0, 43.0, 42.0, 43.0, 42.0, 42.0, 42.0, 42.0, 43.0, 41.0, 41.0, 40.0, 41.0, 40.0, 41.0, 41.0, 40.0, 39.0, 41.0, 41.0, 39.0, 40.0, 41.0, 39.0, 40.0, 40.0, 39.0, 39.0, 40.0, 40.0, 39.0, 40.0, 41.0, 39.0, 39.0, 40.0, 40.0, 39.0, 39.0, 39.0, 39.0, 40.0, 39.0, 39.0, 40.0, 39.0, 39.0, 40.0, 38.0, 38.0, 39.0, 39.0, 38.0, 39.0, 38.0, 39.0, 39.0, 38.0, 39.0, 39.0, 39.0, 38.0, 38.0, 39.0, 39.0, 40.0, 38.0, 40.0, 39.0, 39.0, 39.0, 38.0, 39.0, 38.0, 38.0, 39.0, 40.0, 40.0, 39.0, 38.0, 39.0, 38.0, 38.0, 39.0, 40.0, 39.0, 38.0, 39.0, 39.0, 39.0, 39.0, 39.0, 39.0, 39.0, 39.0, 39.0, 38.0, 38.0, 38.0, 39.0, 38.0, 39.0, 39.0, 38.0, 38.0, 37.0, 39.0, 37.0, 38.0, 39.0, 39.0, 38.0, 37.0, 38.0, 39.0, 39.0, 40.0, 38.0, 38.0, 39.0, 39.0, 40.0, 39.0, 40.0, 38.0, 39.0, 39.0, 40.0, 39.0, 40.0, 41.0, 40.0, 38.0, 40.0, 40.0, 39.0, 40.0, 40.0, 40.0, 41.0, 41.0, 40.0, 41.0, 40.0, 41.0, 42.0, 41.0, 41.0, 41.0, 41.0, 41.0, 42.0, 41.0, 41.0, 43.0, 42.0, 42.0, 41.0, 42.0, 42.0, 42.0, 42.0, 42.0, 43.0, 43.0, 42.0, 42.0, 42.0, 43.0, 43.0, 43.0, 44.0, 43.0, 44.0, 43.0, 43.0, 44.0, 44.0, 43.0, 44.0, 44.0, 44.0, 44.0, 45.0, 44.0, 45.0, 44.0, 45.0, 45.0, 45.0, 45.0, 45.0, 46.0, 46.0, 46.0, 45.0, 46.0, 45.0, 47.0, 46.0, 46.0, 44.0, 47.0, 46.0, 46.0, 47.0, 46.0, 46.0, 46.0, 46.0, 47.0, 46.0, 47.0, 47.0, 47.0, 47.0, 47.0, 47.0, 46.0, 47.0, 46.0, 46.0, 47.0, 47.0, 48.0, 47.0, 48.0, 39.0, 40.0, 39.0, 38.0, 40.0, 40.0, 39.0, 39.0, 39.0, 39.0, 38.0, 38.0, 39.0, 38.0, 39.0, 39.0, 39.0, 39.0, 39.0, 40.0, 40.0, 40.0, 40.0, 40.0, 39.0, 40.0, 39.0, 40.0, 40.0, 40.0, 39.0, 39.0, 40.0, 40.0, 41.0, 41.0, 40.0, 40.0, 41.0, 40.0, 40.0, 40.0, 39.0, 40.0, 40.0, 40.0, 40.0, 40.0, 41.0, 40.0, 40.0, 41.0, 40.0, 41.0, 32.0, 33.0, 33.0, 32.0, 31.0, 32.0, 33.0, 32.0, 33.0, 31.0, 33.0, 32.0, 33.0, 31.0, 32.0, 31.0, 32.0, 32.0, 31.0, 32.0, 32.0, 30.0, 33.0, 31.0, 32.0, 32.0, 33.0, 32.0, 31.0, 32.0, 32.0, 33.0, 32.0, 33.0, 32.0, 32.0, 34.0, 33.0, 32.0, 31.0, 32.0, 32.0, 32.0, 32.0, 30.0, 32.0, 32.0, 31.0, 32.0, 32.0, 33.0, 31.0, 33.0, 32.0, 32.0, 33.0, 32.0, 33.0, 33.0, 32.0, 35.0, 32.0, 32.0, 33.0, 34.0, 33.0, 32.0, 32.0, 33.0, 32.0, 34.0, 33.0, 33.0, 33.0, 32.0, 34.0, 33.0, 32.0, 35.0, 32.0, 32.0, 32.0, 32.0, 32.0, 32.0, 32.0, 30.0, 31.0, 32.0, 30.0, 33.0, 32.0, 33.0, 32.0, 32.0, 32.0, 32.0, 32.0, 33.0, 32.0, 33.0, 31.0, 32.0, 32.0, 32.0, 32.0, 32.0, 32.0, 32.0, 32.0, 32.0, 34.0, 32.0, 34.0, 32.0, 32.0, 33.0, 32.0, 32.0, 32.0, 33.0, 34.0, 32.0, 32.0, 32.0, 32.0, 33.0, 31.0, 32.0, 33.0, 32.0, 33.0, 32.0, 32.0, 33.0, 34.0, 33.0, 33.0, 34.0, 33.0, 32.0, 33.0, 33.0, 28.0, 30.0, 30.0, 28.0, 28.0, 29.0, 28.0, 28.0, 28.0, 27.0, 29.0, 28.0, 29.0, 29.0, 28.0, 29.0, 29.0, 28.0, 29.0, 28.0, 29.0, 27.0, 28.0, 28.0, 28.0, 29.0, 28.0, 28.0, 28.0, 28.0, 28.0, 29.0, 28.0, 28.0, 29.0, 29.0, 27.0, 28.0, 27.0, 28.0, 29.0, 28.0, 28.0, 28.0, 29.0, 28.0, 28.0, 27.0, 28.0, 28.0, 28.0, 29.0, 30.0, 28.0, 28.0, 28.0, 27.0, 28.0, 28.0, 29.0, 28.0, 29.0, 28.0, 28.0, 28.0, 30.0, 29.0, 28.0, 29.0, 29.0, 28.0, 28.0, 28.0, 28.0, 29.0, 29.0, 29.0, 29.0, 29.0, 28.0, 29.0, 28.0, 28.0, 29.0, 28.0, 29.0, 29.0, 28.0, 28.0, 29.0, 28.0, 28.0, 29.0, 29.0, 29.0, 28.0, 29.0, 30.0, 28.0, 28.0, 29.0, 28.0, 29.0, 30.0, 29.0, 30.0, 29.0, 29.0, 29.0, 29.0, 29.0, 30.0, 29.0, 30.0, 29.0, 29.0, 30.0, 29.0, 29.0, 29.0, 29.0, 23.0, 24.0, 23.0, 23.0, 24.0, 24.0, 22.0, 23.0, 24.0, 24.0, 24.0, 24.0, 24.0, 23.0, 24.0, 24.0, 23.0, 23.0, 24.0, 23.0, 23.0, 23.0, 24.0, 23.0, 23.0, 23.0, 24.0, 24.0, 23.0, 23.0, 23.0, 24.0, 23.0, 23.0, 24.0, 24.0, 23.0, 23.0, 24.0, 23.0, 24.0, 22.0, 24.0, 23.0, 24.0, 24.0, 23.0, 23.0, 24.0, 25.0, 24.0, 23.0, 25.0, 23.0, 23.0, 24.0, 23.0, 24.0, 23.0, 23.0, 24.0, 24.0, 23.0, 24.0, 23.0, 24.0, 24.0, 24.0, 23.0, 24.0, 24.0, 23.0, 24.0, 23.0, 24.0, 25.0, 24.0, 24.0, 25.0, 23.0, 23.0, 24.0, 23.0, 24.0, 23.0, 24.0, 24.0, 24.0, 24.0, 24.0, 24.0, 25.0, 24.0, 24.0, 23.0, 24.0, 25.0, 23.0, 24.0, 23.0, 23.0, 24.0, 24.0, 24.0, 25.0, 24.0, 25.0, 24.0, 24.0, 24.0, 23.0, 24.0, 23.0, 23.0, 24.0, 24.0, 23.0, 24.0, 23.0, 22.0};
        double[] b = new double[]{-109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -109.0, -108.0, -108.0, -108.0, -108.0, -107.0, -107.0, -107.0, -106.0, -106.0, -106.0, -105.0, -105.0, -105.0, -104.0, -104.0, -104.0, -103.0, -103.0, -103.0, -102.0, -102.0, -102.0, -102.0, -101.0, -101.0, -101.0, -101.0, -101.0, -100.0, -100.0, -100.0, -100.0, -100.0, -100.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -98.0, -98.0, -98.0, -98.0, -98.0, -98.0, -98.0, -98.0, -98.0, -97.0, -97.0, -97.0, -97.0, -97.0, -97.0, -97.0, -97.0, -97.0, -97.0, -97.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -96.0, -95.0, -96.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -94.0, -95.0, -95.0, -94.0, -94.0, -95.0, -95.0, -95.0, -95.0, -95.0, -94.0, -95.0, -94.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -95.0, -94.0, -94.0, -94.0, -94.0, -94.0, -95.0, -94.0, -94.0, -95.0, -94.0, -94.0, -95.0, -94.0, -94.0, -95.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -94.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -70.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -69.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -45.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -41.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -27.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, -9.0, 0.0, 0.0};
        double[] c = new double[]{19.0, 20.0, 20.0, 20.0, 20.0, 20.0, 21.0, 21.0, 21.0, 21.0, 22.0, 22.0, 22.0, 22.0, 22.0, 23.0, 23.0, 23.0, 23.0, 23.0, 24.0, 24.0, 24.0, 24.0, 25.0, 25.0, 25.0, 25.0, 25.0, 26.0, 26.0, 26.0, 26.0, 26.0, 27.0, 27.0, 27.0, 27.0, 27.0, 28.0, 28.0, 28.0, 28.0, 29.0, 29.0, 29.0, 29.0, 29.0, 30.0, 30.0, 30.0, 30.0, 30.0, 31.0, 31.0, 31.0, 31.0, 31.0, 32.0, 32.0, 32.0, 32.0, 32.0, 33.0, 33.0, 33.0, 33.0, 33.0, 34.0, 34.0, 34.0, 34.0, 34.0, 35.0, 35.0, 35.0, 35.0, 35.0, 36.0, 36.0, 36.0, 36.0, 36.0, 36.0, 37.0, 37.0, 37.0, 37.0, 37.0, 38.0, 38.0, 38.0, 38.0, 38.0, 39.0, 39.0, 39.0, 39.0, 39.0, 40.0, 40.0, 40.0, 40.0, 40.0, 41.0, 41.0, 41.0, 41.0, 41.0, 41.0, 42.0, 42.0, 42.0, 42.0, 42.0, 43.0, 43.0, 43.0, 43.0, 43.0, 44.0, 44.0, 44.0, 44.0, 44.0, 45.0, 45.0, 45.0, 45.0, 45.0, 45.0, 46.0, 46.0, 46.0, 46.0, 46.0, 47.0, 47.0, 47.0, 47.0, 47.0, 48.0, 48.0, 48.0, 48.0, 48.0, 49.0, 49.0, 49.0, 49.0, 49.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 51.0, 51.0, 51.0, 51.0, 51.0, 52.0, 52.0, 52.0, 52.0, 52.0, 53.0, 53.0, 53.0, 53.0, 53.0, 54.0, 54.0, 54.0, 54.0, 54.0, 55.0, 55.0, 55.0, 55.0, 55.0, 56.0, 56.0, 56.0, 56.0, 56.0, 57.0, 57.0, 57.0, 57.0, 57.0, 58.0, 58.0, 58.0, 58.0, 58.0, 58.0, 59.0, 59.0, 59.0, 59.0, 59.0, 60.0, 60.0, 60.0, 60.0, 60.0, 61.0, 61.0, 61.0, 61.0, 61.0, 62.0, 62.0, 62.0, 62.0, 62.0, 63.0, 63.0, 63.0, 63.0, 63.0, 63.0, 64.0, 64.0, 64.0, 64.0, 64.0, 65.0, 65.0, 65.0, 65.0, 65.0, 66.0, 66.0, 66.0, 66.0, 66.0, 67.0, 67.0, 67.0, 67.0, 67.0, 68.0, 68.0, 68.0, 68.0, 68.0, 69.0, 69.0, 69.0, 69.0, 69.0, 69.0, 69.0, 70.0, 70.0, 70.0, 70.0, 70.0, 70.0, 71.0, 71.0, 71.0, 71.0, 71.0, 71.0, 71.0, 72.0, 72.0, 72.0, 72.0, 72.0, 72.0, 72.0, 73.0, 73.0, 73.0, 73.0, 73.0, 73.0, 73.0, 74.0, 74.0, 74.0, 74.0, 74.0, 74.0, 74.0, 75.0, 75.0, 75.0, 75.0, 75.0, 75.0, 76.0, 76.0, 76.0, 76.0, 76.0, 76.0, 76.0, 77.0, 77.0, 77.0, 77.0, 77.0, 77.0, 77.0, 77.0, 77.0, 78.0, 78.0, 78.0, 78.0, 78.0, 78.0, 78.0, 78.0, 78.0, 79.0, 79.0, 79.0, 79.0, 79.0, 79.0, 79.0, 79.0, 79.0, 79.0, 80.0, 80.0, 80.0, 80.0, 80.0, 80.0, 80.0, 80.0, 80.0, 80.0, 81.0, 81.0, 81.0, 81.0, 81.0, 81.0, 81.0, 81.0, 81.0, 81.0, 82.0, 82.0, 82.0, 82.0, 82.0, 82.0, 82.0, 82.0, 82.0, 82.0, 83.0, 83.0, 83.0, 83.0, 83.0, 83.0, 83.0, 83.0, 83.0, 83.0, 84.0, 84.0, 84.0, 84.0, 84.0, 84.0, 84.0, 84.0, 84.0, 85.0, 85.0, 85.0, 85.0, 85.0, 85.0, 85.0, 85.0, 85.0, 85.0, 85.0, 86.0, 86.0, 86.0, 86.0, 86.0, 86.0, 86.0, 86.0, 86.0, 86.0, 86.0, 87.0, 87.0, 87.0, 87.0, 87.0, 87.0, 87.0, 87.0, 87.0, 87.0, 88.0, 88.0, 88.0, 88.0, 88.0, 88.0, 88.0, 88.0, 88.0, 88.0, 88.0, 89.0, 89.0, 89.0, 89.0, 89.0, 89.0, 89.0, 89.0, 89.0, 89.0, 89.0, 89.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 91.0, 91.0, 91.0, 91.0, 91.0, 91.0, 91.0, 91.0, 91.0, 91.0, 91.0, 91.0, 91.0, 91.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 92.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 93.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 94.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0, 96.0, 96.0, 96.0, 96.0, 96.0, 96.0, 96.0, 96.0, 96.0, 96.0, 96.0, 96.0, 96.0, 96.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 97.0, 98.0, 98.0, 98.0, 98.0, 98.0, 98.0, 98.0, 98.0, 98.0, 98.0, 98.0, 98.0, 98.0, 98.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 99.0, 100.0, 100.0};
        int[] d = {1,2,3,4,5};
        System.out.println(new PlatformAlgorithm().bmsSamplingAnomaly(222221561,d, d,  80, 20));
    }


}
