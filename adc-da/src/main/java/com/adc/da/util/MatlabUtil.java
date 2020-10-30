package com.adc.da.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.adc.da.util.MathUtil;

public class MatlabUtil {


    /**
     * 箱线图及对应倍数阀值
     *
     * @param x
     * @param flag
     * @return
     */
    public static double[][] boxoutlier(double[][] x, int flag) {

        double[][] result = new double[0][];
        double Q1 = prctile(x, 25);
        double Q3 = prctile(x, 75);
        double Q0 = MathUtil.subtractDouble(Q1, flag * MathUtil.subtractDouble(Q3, Q1));
        double Q4 = MathUtil.addDouble(Q3, flag * MathUtil.subtractDouble(Q3, Q1));
        result = boxoutlierFind(x, Q0, Q4);

        return result;

    }

    /**
     * 一维数据版本
     *
     * @param value
     * @param win   (默认依据数组从0开始)
     * @return
     */
    public static double[] medianSmooth(double[] value, int win) {

        int n = value.length;
        double[] result = new double[n];
        result[0] = value[0];
        for (int i = 1; i < n - 1; i++) {
            if (i < win) {
                result[i] = median(matrixStartStop(value, 0, 2 * i), 1);
            } else if (i >= n - win) {
                result[i] = median(matrixStartStop(value, 2 * i - n + 1, n - 1), 1);
            } else {
                result[i] = median(matrixStartStop(value, i - win, i + win), 1);
            }
        }
        result[n - 1] = value[n - 1];
        return result;

    }

    /**
     * 二维数据版本
     *
     * @param value
     * @param win   (默认依据数组从0开始)
     * @return
     */
    public static double[][] medianSmooth(double[][] value, int win) {

        int n = length(value);  /* 获取行数和列数中的较大值*/
        double[][] result = new double[n][1];
        result[0][0] = value[0][0];
        for (int i = 1; i < n - 1; i++) {
            if (i <= win) {
                result[i][0] = median(matrixStartStop(value, 0, 2 * i), 1)[0][0];
            } else if (i > n - win - 1) {
                result[i][0] = median(matrixStartStop(value, 2 * i - n - 1, n - 1), 1)[0][0];
            } else {
                result[i][0] = median(matrixStartStop(value, i - win, i + win), 1)[0][0];
            }
        }
        result[n - 1][0] = value[n - 1][0];
        return result;

    }


    /**
     * 获取矩阵行数和列数中的较大值
     *
     * @param value
     * @return
     */
    public static int length(double[][] value) {

        int[] size = size(value);   /* 获取矩阵的行数和列数*/
        return size[0] > size[1] ? size[0] : size[1];   /* 返回较大值*/

    }


    /**
     * 返回矩阵的行数和列数
     *
     * @param value
     * @return
     */
    public static int[] size(double[][] value) {

        int[] result = {value.length, value[0].length};
        return result;

    }


    /**
     * 判断是否为空
     *
     * @param value
     * @return
     */
    public static boolean isEmpty(double[][] value) {

        if (value == null) {
            return true;
        } else {
            int rowLength = value.length;
            int columnLength = value[0].length;
            if (rowLength != 0 && columnLength != 0) {
                return false;
            } else {
                return true;
            }
        }

    }


    /**
     * 求和
     *
     * @param value
     * @return
     */
    public static double sum(double[] value) {

        double result = 0;
        int valueLength = value.length;
        for (int i = 0; i < valueLength; i++) {
//            result += value[i];
            result = MathUtil.addDouble(result, value[i]);  /* 解决精度丢失问题*/
        }
        return result;

    }


    /**
     * matlab  sum函数求和（为矩阵时：每列进行求和，返回行向量；为行向量时，行进行求和；为列向量时，列进行求和）
     *
     * @param value
     * @return
     */
    public static double[][] sum(double[][] value) {

        int rowLength = value.length;        /* 行数*/
        int columnLength = value[0].length;  /* 列数*/
        double[][] result = new double[1][1];  /* 返回结果*/

        if (rowLength > 1 && columnLength > 1) {               /* 为矩阵时*/
            result = new double[1][columnLength];
            for (int column = 0; column < columnLength; column++) {
                double columnTotal = 0;
                for (int row = 0; row < rowLength; row++) {
                    columnTotal += value[row][column];
                }
                result[0][column] = columnTotal;
            }
        } else if (rowLength == 1 && columnLength > 1) {       /* 为行向量时*/
            result = new double[1][1];
            double rowTotal = 0;
            for (int column = 0; column < columnLength; column++) {
                rowTotal += value[0][column];
            }
            result[0][0] = rowTotal;
        } else if (rowLength > 1 && columnLength == 1) {       /* 为列向量时*/
            result = new double[1][1];
            double columnTotal = 0;
            for (int row = 0; row < rowLength; row++) {
                columnTotal += value[row][0];
            }
            result[0][0] = columnTotal;
        }
        return result;

    }


    /**
     * 求平均值（无其他参数）
     *
     * @param value
     * @return
     */
    public static double mean(double[] value) {

        int valueLength = value.length;/* 获取行数*/
        double result;/* 一维数组平均值*/
        double totalValue = 0;
        /* 循环遍历列*/
        for (int i = 0; i < valueLength; i++) {
//            totalValue += value[i];
            totalValue = MathUtil.addDouble(totalValue, value[i]);
        }
//        result = totalValue / valueLength;
        result = MathUtil.divideDouble(totalValue, valueLength);
        return result;

    }


    /**
     * 求平均值（无其他参数）
     *
     * @param value
     * @return
     */
    public static double[][] mean(double[][] value) {

        int rowLength = value.length;/* 获取行数*/
        int columnLength = value[0].length;/* 获取列数*/
        double[][] result = new double[1][1];/* 一维数组平均值*/
        if (rowLength == 1 && columnLength > 1) {            /* 当数据为行向量时*/
            result = new double[1][1];
            double totalValue = 0;
            /* 循环遍历列*/
            for (int column = 0; column < columnLength; column++) {
                totalValue += value[0][column];
            }
            result[0][0] = totalValue / columnLength;
        } else if (rowLength > 1 && columnLength == 1) {     /* 当数据为列向量时*/
            result = new double[1][1];
            double totalValue = 0;
            /* 循环遍历行*/
            for (int row = 0; row < rowLength; row++) {
                totalValue += value[row][0];
            }
            result[0][0] = totalValue / rowLength;
        } else if (rowLength > 1 && columnLength > 1) {      /* 当数据为矩阵时*/
            result = new double[1][columnLength];
            /* 循环遍历列*/
            for (int column = 0; column < columnLength; column++) {
                double columnTotalValue = 0;/* 列值求和*/
                /* 循环遍历行*/
                for (int row = 0; row < rowLength; row++) {
                    columnTotalValue += value[row][column];/* 列值求和*/
                }
                result[0][column] = columnTotalValue / rowLength;/* 求平均值*/
            }
        }
        return result;

    }


    /**
     * 求平均值（列元素求平均值，返回一维数组）
     *
     * @param value
     * @param dim   数据为矩阵时起作用 1-列，竖着求平均值； 2-行，横着求平均值
     * @return
     */
    public static double[][] mean(double[][] value, int dim) {

        int rowLength = value.length;/* 获取行数*/
        int columnLength = value[0].length;/* 获取列数*/
        double[][] result = new double[1][1];/* 一维数组平均值*/
        if (rowLength == 1 && columnLength > 1) {            /* 当数据为行向量时*/
            result = new double[1][1];
            double totalValue = 0;
            /* 循环遍历列*/
            for (int column = 0; column < columnLength; column++) {
//                totalValue += value[0][column];
                totalValue = MathUtil.addDouble(totalValue, value[0][column]);
            }
//            result[0][0] = totalValue / columnLength;
            result[0][0] = MathUtil.divideDouble(totalValue, columnLength);
        } else if (rowLength > 1 && columnLength == 1) {     /* 当数据为列向量时*/
            result = new double[1][1];
            double totalValue = 0;
            /* 循环遍历行*/
            for (int row = 0; row < rowLength; row++) {
//                totalValue += value[row][0];
                totalValue = MathUtil.addDouble(totalValue, value[row][0]);
            }
//            result[0][0] = totalValue / rowLength;
            result[0][0] = MathUtil.divideDouble(totalValue, rowLength);
        } else if (rowLength > 1 && columnLength > 1) {      /* 当数据为矩阵时*/
            if (dim == 1) {                           /* 列，竖着取平均值*/
                result = new double[1][columnLength];
                /* 循环遍历列*/
                for (int column = 0; column < columnLength; column++) {
                    double columnTotalValue = 0;/* 列值求和*/
                    /* 循环遍历行*/
                    for (int row = 0; row < rowLength; row++) {
//                        columnTotalValue += value[row][column];/* 列值求和*/
                        columnTotalValue = MathUtil.addDouble(columnTotalValue, value[row][column]);
                    }
//                    result[0][column] = columnTotalValue / rowLength;/* 求平均值*/
                    result[0][column] = MathUtil.divideDouble(columnTotalValue, rowLength);
                }
            } else if (dim == 2) {                  /* 行，横着取平均值*/
                result = new double[rowLength][1];
                /* 循环遍历行*/
                for (int row = 0; row < rowLength; row++) {
                    double rowTotalValue = 0;/* 列值求和*/
                    /* 循环遍历列*/
                    for (int column = 0; column < columnLength; column++) {
//                        rowTotalValue += value[row][column];/* 列值求和*/
                        rowTotalValue = MathUtil.addDouble(rowTotalValue, value[row][column]);
                    }
//                    result[row][0] = rowTotalValue / columnLength;/* 求平均值*/
                    result[row][0] = MathUtil.divideDouble(rowTotalValue, columnLength);
                }
            }

        }
        return result;

    }


    /**
     * 一维数据：求差分
     *
     * @param value
     * @return
     */
    public static double[] diff(double[] value) {

        int valueLength = value.length;
        double[] result = new double[valueLength - 1];
        for (int i = 0; i < valueLength - 1; i++) {
            result[i] = MathUtil.subtractDouble(value[i + 1], value[i]);
        }
        return result;

    }


    /**
     * 二维矩阵：求差分
     *
     * @param value
     * @return
     */
    public static double[][] diff(double[][] value) {

        int rowLength = value.length;
        int columnLength = value[0].length;
        double[][] result = new double[rowLength][columnLength];

        if (rowLength == 1 && columnLength > 1) {         /* 为行向量*/
            result = new double[1][columnLength - 1];
            for (int column = 0; column < columnLength - 1; column++) {
                result[0][column] = value[0][column + 1] - value[0][column];
            }
        } else if (rowLength > 1 && columnLength == 1) {  /* 为列向量*/
            result = new double[rowLength - 1][1];
            for (int row = 0; row < rowLength - 1; row++) {
                result[row][0] = value[row + 1][0] - value[row][0];
            }
        } else if (rowLength > 1 && columnLength > 1) {   /* 为矩阵*/
            result = new double[rowLength - 1][columnLength];
            for (int row = 0; row < rowLength - 1; row++) {
                for (int column = 0; column < columnLength; column++) {
                    result[row][column] = value[row + 1][column] - value[row][column];
                }
            }
        }

        return result;

    }


    /**
     * 矩阵行列转换
     *
     * @param value
     * @return
     */
    public static double[][] unpiovt(double[][] value) {

        int rowLength = value.length;/* 获取行数*/
        int columnLength = value[0].length;/* 获取列数*/
        double[][] result = new double[columnLength][rowLength];/* 一维数组平均值*/
        //循环遍历行
        for (int row = 0; row < rowLength; row++) {
            //循环遍历列
            for (int column = 0; column < columnLength; column++) {
                result[column][row] = value[row][column];
            }
        }
        return result;

    }

    /**
     * 获取行最小值
     *
     * @param value
     * @return
     */
    public static double min(double[] value) {

        double length = value.length;   /* 行最小值*/
        double min = value[0];
        for (int i = 0; i < length; i++) {  /* 遍历行向量*/
            min = min < value[i] ? min : value[i];  /* 获取最大值*/
        }
        return min;

    }


    /**
     * 获取行向量、列向量或矩阵的最小值
     *
     * @param value 行向量、列向量或矩阵
     * @return
     */
    public static double[][] min(double[][] value) {

        int rowLength = value.length; /* 获取矩阵的行数*/
        int columnLength = value[0].length; /* 获取矩阵的列数*/
        double[][] result = new double[1][1]; /* 返回结果*/

        if (rowLength == 1 && columnLength > 1) {         /* 该数据为行向量*/
            double minCellValue = value[0][0];   /* 行最小值*/
            for (int column = 0; column < columnLength; column++) {  /* 遍历行向量*/
                minCellValue = minCellValue < value[0][column] ? minCellValue : value[0][column];  /* 获取最小值*/
            }
            result[0][0] = minCellValue;
        } else if (rowLength > 1 && columnLength == 1) {  /* 该数据为列向量*/
            double minCellValue = value[0][0]; /* 列最小值*/
            for (int row = 0; row < rowLength; row++) {  /* 遍历列向量*/
                minCellValue = minCellValue < value[row][0] ? minCellValue : value[row][0];  /* 获取最小值*/
            }
            result[0][0] = minCellValue;
        } else if (rowLength > 1 && columnLength > 1) {   /* 该数据为矩阵*/
            result = new double[1][columnLength];
            for (int column = 0; column < columnLength; column++) {  /* 遍历列*/
                double minCellValue = value[0][column];    /* 列最小值*/
                for (int row = 0; row < rowLength; row++) {  /* 遍历行*/
                    minCellValue = minCellValue < value[row][column] ? minCellValue : value[row][column];  /* 获取最小值*/
                }
                result[0][column] = minCellValue;
            }
        }
        return result;
    }


    /**
     * 获取最大值
     *
     * @param value
     * @return
     */
    public static double max(double[] value) {

        double length = value.length;   /* 行最小值*/
        double max = value[0];
        for (int i = 0; i < length; i++) {  /* 遍历行向量*/
            max = max > value[i] ? max : value[i];  /* 获取最大值*/
        }
        return max;

    }

    /**
     * 获取最大值
     *
     * @param value
     * @return
     */
    public static double max(int[] value) {

        double length = value.length;   /* 行最小值*/
        double max = value[0];
        for (int i = 0; i < length; i++) {  /* 遍历行向量*/
            max = max > value[i] ? max : value[i];  /* 获取最大值*/
        }
        return max;

    }


    /**
     * 获取行向量、列向量或矩阵的最大值
     *
     * @param value 行向量、列向量或矩阵
     * @return
     */
    public static double[][] max(double[][] value) {
        int rowLength = value.length; /* 获取矩阵的行数*/
        int columnLength = value[0].length; /* 获取矩阵的列数*/
        double[][] result = new double[1][1];

        if (rowLength == 1 && columnLength > 1) {         /* 该数据为行向量*/
            double maxCellValue = value[0][0];   /* 行最小值*/
            for (int column = 0; column < columnLength; column++) {  /* 遍历行向量*/
                maxCellValue = maxCellValue > value[0][column] ? maxCellValue : value[0][column];  /* 获取最大值*/
            }
            result[0][0] = maxCellValue;
        } else if (rowLength > 1 && columnLength == 1) {  /* 该数据为列向量*/
            double maxCellValue = value[0][0]; /* 列最小值*/
            for (int row = 0; row < rowLength; row++) {  /* 遍历列向量*/
                maxCellValue = maxCellValue > value[row][0] ? maxCellValue : value[row][0];  /* 获取最大值*/
            }
            result[0][0] = maxCellValue;
        } else if (rowLength > 1 && columnLength > 1) {   /* 该数据为矩阵*/
            result = new double[1][columnLength]; /* 返回结果*/
            for (int column = 0; column < columnLength; column++) {  /* 遍历列*/
                double maxCellValue = value[0][column];    /* 列最小值*/
                for (int row = 0; row < rowLength; row++) {  /* 遍历行*/
                    maxCellValue = maxCellValue > value[row][column] ? maxCellValue : value[row][column];  /* 获取最大值*/
                }
                result[0][column] = maxCellValue;
            }
        }
        return result;

    }


    /**
     * 一维数据：求绝对值
     *
     * @param value
     * @return
     */
    public static double[] abs(double[] value) {

        int valueLength = value.length;
        double[] result = new double[valueLength];
        for (int i = 0; i < valueLength; i++) {
            result[i] = Math.abs(value[i]);
        }
        return result;

    }


    /**
     * 二维数据：求绝对值
     *
     * @param value
     * @return
     */
    public static double[][] abs(double[][] value) {

        int rowLength = value.length; /* 获取矩阵的行数*/
        int columnLength = value[0].length; /* 获取矩阵的列数*/

        for (int row = 0; row < rowLength; row++) {
            for (int column = 0; column < columnLength; column++) {
                value[row][column] = Math.abs(value[row][column]);
            }
        }
        return value;

    }


    /**
     * 加法-两个矩阵相加
     *
     * @param value1
     * @param value2
     * @return
     */
    public static double[][] add(double[][] value1, double[][] value2) {

        int value1RowLength = value1.length; /* value1行数*/
        int value1Column1Length = value1[0].length; /* value1列数*/
        int value2RowLength = value2.length; /* value2行数*/
        int value2Column1Length = value2[0].length; /* alue2列数*/
        int maxRowLength = value1RowLength > value2RowLength ? value1RowLength : value2RowLength; /* 获取行数最大值*/
        int maxColumnLength = value1Column1Length > value2Column1Length ? value1Column1Length : value2Column1Length; /* 获取列数最大值*/
        double[][] result = new double[maxRowLength][maxColumnLength]; /* 加法返回值*/

        /* 判断数据格式*/
        if (value1RowLength == 1 && value1Column1Length > 1) {                                                           /* value1为行向量 */
            if (value2RowLength == 1 && value2Column1Length > 1 && value1Column1Length == value2Column1Length) {              /* value2为行向量-矩阵维度必须一致（列数一致）*/
                for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                    result[0][column] = value1[0][column] + value2[0][column];
                }
            } else if (value2RowLength > 1 && value2Column1Length == 1) {                                                   /* value2为列向量*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = value1[0][column] + value2[row][0];
                    }
                }
            } else if (value2RowLength > 1 && value2Column1Length > 1 && value1Column1Length == value2Column1Length) {      /* value2为矩阵-矩阵维度必须一致（列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = value1[0][column] + value2[row][column];
                    }
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }
        } else if (value1RowLength > 1 && value1Column1Length == 1) {                                                  /* value1为列向量*/
            if (value2RowLength == 1 && value2Column1Length > 1) {                                                           /* value2为行向量*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = value1[row][0] + value2[0][column];
                    }
                }
            } else if (value2RowLength > 1 && value2Column1Length == 1 && value1RowLength == value2RowLength) {            /* value2为列向量-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {
                    result[row][0] = value1[row][0] + value2[row][0];
                }
            } else if (value2RowLength > 1 && value2Column1Length > 1 && value1RowLength == value2RowLength) {             /* value2为矩阵-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = value1[row][0] + value2[0][column];
                    }
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }

        } else if (value1RowLength > 1 && value1Column1Length > 1) {                                                   /* value1为矩阵*/
            if (value2RowLength == 1 && value2Column1Length > 1 && value1Column1Length == value2Column1Length) {            /* value2为行向量-矩阵维度必须一致（列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = value1[row][column] + value2[0][column];
                    }
                }
            } else if (value2RowLength > 1 && value2Column1Length == 1 && value1RowLength == value2RowLength) {             /* value2为列向量-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = value1[row][column] + value2[row][0];
                    }
                }
            } else if (value2RowLength > 1 && value2Column1Length > 1 && value1RowLength == value2RowLength && value1Column1Length == value2Column1Length) {   /* value2为矩阵-矩阵维度必须一致（行数、列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = value1[row][column] + value2[row][column];
                    }
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }
        }

        return result;

    }


    /**
     * 减法-两个矩阵相减   ：二维数据
     *
     * @param minuend    被减数
     * @param subtrahend 减数
     * @return
     */
    public static double[][] sub(double[][] minuend, double[][] subtrahend) {

        int minuendRowLength = minuend.length; /* 被减数矩阵的行数*/
        int minuendColumnLength = minuend[0].length; /* 被减数矩阵的列数*/
        int subtrahendRowLength = subtrahend.length; /* 减数矩阵的行数*/
        int subtrahendColumnLength = subtrahend[0].length; /* 减数矩阵的列数*/
        int maxRowLength = subtrahendRowLength > minuendRowLength ? subtrahendRowLength : minuendRowLength; /* 获取行数最大值*/
        int maxColumnLength = subtrahendColumnLength > minuendColumnLength ? subtrahendColumnLength : minuendColumnLength; /* 获取列数最大值*/
        double[][] result = new double[maxRowLength][maxColumnLength]; /* 减法返回值*/

        /* 判断数据格式*/
        if (minuendRowLength == 1 && minuendColumnLength > 1) {                                                           /* minuend为行向量 */
            if (subtrahendRowLength == 1 && subtrahendColumnLength > 1 && minuendColumnLength == subtrahendColumnLength) {              /* subtrahend为行向量-矩阵维度必须一致（列数一致）*/
                for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
//                    result[0][column] = minuend[0][column] - subtrahend[0][column];
                    result[0][column] = MathUtil.subtractDouble(minuend[0][column], subtrahend[0][column]);    /* 减法*/
                }
            } else if (subtrahendRowLength > 1 && subtrahendColumnLength == 1) {                                                      /* subtrahend为列向量*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
//                        result[row][column] = minuend[0][column] - subtrahend[row][0];
                        result[row][column] = MathUtil.subtractDouble(minuend[0][column], subtrahend[row][0]);    /* 减法*/
                    }
                }
            } else if (subtrahendRowLength > 1 && subtrahendColumnLength > 1 && minuendColumnLength == subtrahendColumnLength) {      /* subtrahend为矩阵-矩阵维度必须一致（列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
//                        result[row][column] = minuend[0][column] - subtrahend[row][column];
                        result[row][column] = MathUtil.subtractDouble(minuend[0][column], subtrahend[row][column]);  /* 减法*/
                    }
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }
        } else if (minuendRowLength > 1 && minuendColumnLength == 1) {                                                  /* minuend为列向量*/
            if (subtrahendRowLength == 1 && subtrahendColumnLength > 1) {                                                             /* subtrahend为行向量*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
//                        result[row][column] = minuend[row][0] - subtrahend[0][column];
                        result[row][column] = MathUtil.subtractDouble(minuend[row][0], subtrahend[0][column]);   /* 减法*/
                    }
                }
            } else if (subtrahendRowLength > 1 && subtrahendColumnLength == 1 && minuendRowLength == subtrahendRowLength) {            /* subtrahend为列向量-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {
//                    result[row][0] = minuend[row][0] - subtrahend[row][0];
                    result[row][0] = MathUtil.subtractDouble(minuend[row][0], subtrahend[row][0]);         /* 减法*/
                }
            } else if (subtrahendRowLength > 1 && subtrahendColumnLength > 1 && minuendRowLength == subtrahendRowLength) {             /* subtrahend为矩阵-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
//                        result[row][column] = minuend[row][0] - subtrahend[0][column];
                        result[row][column] = MathUtil.subtractDouble(minuend[row][0], subtrahend[0][column]);      /* 减法*/
                    }
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }

        } else if (minuendRowLength > 1 && minuendColumnLength > 1) {                                                    /* minuend为矩阵*/
            if (subtrahendRowLength == 1 && subtrahendColumnLength > 1 && minuendColumnLength == subtrahendColumnLength) {             /* subtrahend为行向量-矩阵维度必须一致（列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
//                        result[row][column] = minuend[row][column] - subtrahend[0][column];
                        result[row][column] = MathUtil.subtractDouble(minuend[row][column], subtrahend[0][column]);   /* 减法*/
                    }
                }
            } else if (subtrahendRowLength > 1 && subtrahendColumnLength == 1 && minuendRowLength == subtrahendRowLength) {             /* subtrahend为列向量-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
//                        result[row][column] = minuend[row][column] - subtrahend[row][0];
                        result[row][column] = MathUtil.subtractDouble(minuend[row][column], subtrahend[row][0]);       /* 减法*/
                    }
                }
            } else if (subtrahendRowLength > 1 && subtrahendColumnLength > 1 && minuendRowLength == subtrahendRowLength && minuendColumnLength == subtrahendColumnLength) {   /* subtrahend为矩阵-矩阵维度必须一致（行数、列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
//                        result[row][column] = minuend[row][column] - subtrahend[row][column];
                        result[row][column] = MathUtil.subtractDouble(minuend[row][column], subtrahend[row][column]);  /* 减法*/
                    }
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }
        }

        return result;

    }


    /**
     * 减法-两个矩阵相减  :一维数据
     *
     * @param minuend    被减数
     * @param subtrahend 减数
     * @return
     */
    public static double[] sub(double[] minuend, double[] subtrahend) {

        int minuendColumnLength = minuend.length; /* 被减数矩阵的列数*/
        int subtrahendColumnLength = subtrahend.length; /* 减数矩阵的列数*/
        int maxColumnLength = subtrahendColumnLength > minuendColumnLength ? subtrahendColumnLength : minuendColumnLength; /* 获取列数最大值*/
        double[] result = new double[maxColumnLength]; /* 减法返回值*/

        for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
            result[column] = MathUtil.subtractDouble(minuend[column], subtrahend[column]);    /* 减法*/
        }
        return result;

    }


    /**
     * 两个日期相减获取 相差的天数（double）
     *
     * @param minuendTime    被减数
     * @param subtrahendTime 减数
     * @return 二维数据
     */
    public static double[][] dateSub(String minuendTime, String subtrahendTime) throws ParseException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date minuendDate = simpleDateFormat.parse(minuendTime);
        Date subtrahendDate = simpleDateFormat.parse(subtrahendTime);
        double[][] subDate = new double[1][1];
        subDate[0][0] = (minuendDate.getTime() - subtrahendDate.getTime()) / (24 * 3600 * 1000);
        return subDate;
    }

    /**
     * 两个日期相减获取 相差的天数(double)
     *
     * @param minuendTime    被减数
     * @param subtrahendTime 减数
     * @return 一维数据
     */
    public static double singleDateSub(String minuendTime, String subtrahendTime) throws ParseException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date minuendDate = simpleDateFormat.parse(minuendTime);
        Date subtrahendDate = simpleDateFormat.parse(subtrahendTime);
//        subDate[0] = (minuendDate.getTime() - subtrahendDate.getTime()) / (24 * 3600 * 1000);
        double subDate = MathUtil.divideDouble(MathUtil.subtractDouble(minuendDate.getTime(), subtrahendDate.getTime()), 24 * 3600 * 1000);
        return subDate;

    }


    /**
     * 乘法
     *
     * @param multiplicand 被乘数
     * @param multiplier   乘数
     * @return
     */
    public static double[][] multiplication(double[][] multiplicand, double[][] multiplier) {

        int multiplicandRowLength = multiplicand.length;
        int multiplicandColumnLength = multiplicand[0].length;
        int multiplierRowlength = multiplier.length;
        int multiplierColumnLength = multiplier[0].length;
        int maxRowLength = multiplicandRowLength > multiplierRowlength ? multiplicandRowLength : multiplierRowlength; /* 获取行数最大值*/
        int maxColumnLength = multiplicandColumnLength > multiplierColumnLength ? multiplicandColumnLength : multiplierColumnLength; /* 获取列数最大值*/
        double[][] result = new double[maxRowLength][maxColumnLength]; /* 减法返回值*/

        /* 判断数据格式*/
        if (multiplicandRowLength == 1 && multiplicandColumnLength > 1) {                                                           /* multiplicand为行向量 */
            if (multiplierRowlength == 1 && multiplierColumnLength > 1 && multiplicandColumnLength == multiplierColumnLength) {              /* multiplier为行向量-矩阵维度必须一致（列数一致）*/
                for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                    result[0][column] = multiplicand[0][column] * multiplier[0][column];
                }
            } else if (multiplierRowlength > 1 && multiplierColumnLength == 1) {                                                      /* multiplier为列向量*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = multiplicand[0][column] * multiplier[row][0];
                    }
                }
            } else if (multiplierRowlength > 1 && multiplierColumnLength > 1 && multiplicandColumnLength == multiplierColumnLength) {      /* multiplier为矩阵-矩阵维度必须一致（列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = multiplicand[0][column] * multiplier[row][column];
                    }
                }
            } else if (multiplierRowlength == 1 && multiplierColumnLength == 1) {                                                    /* multiplier单一数值时*/
                for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                    result[0][column] = multiplicand[0][column] * multiplier[0][0];
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }
        } else if (multiplicandRowLength > 1 && multiplicandColumnLength == 1) {                                                  /* multiplicand为列向量*/
            if (multiplierRowlength == 1 && multiplierColumnLength > 1) {                                                             /* multiplier为行向量*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = multiplicand[row][0] * multiplier[0][column];
                    }
                }
            } else if (multiplierRowlength > 1 && multiplierColumnLength == 1 && multiplicandRowLength == multiplierRowlength) {            /* multiplier为列向量-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {
                    result[row][0] = multiplicand[row][0] * multiplier[row][0];
                }
            } else if (multiplierRowlength > 1 && multiplierColumnLength > 1 && multiplicandRowLength == multiplierRowlength) {             /* multiplier为矩阵-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = multiplicand[row][0] * multiplier[0][column];
                    }
                }
            } else if (multiplierRowlength == 1 && multiplierColumnLength == 1) {                                                    /* multiplier单一数值时*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    result[row][0] = multiplicand[0][0] * multiplier[0][0];
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }

        } else if (multiplicandRowLength > 1 && multiplicandColumnLength > 1) {                                                    /* multiplicand为矩阵*/
            if (multiplierRowlength == 1 && multiplierColumnLength > 1 && multiplicandColumnLength == multiplierColumnLength) {             /* multiplier为行向量-矩阵维度必须一致（列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = multiplicand[row][column] * multiplier[0][column];
                    }
                }
            } else if (multiplierRowlength > 1 && multiplierColumnLength == 1 && multiplicandRowLength == multiplierRowlength) {             /* multiplier为列向量-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = multiplicand[row][column] * multiplier[row][0];
                    }
                }
            } else if (multiplierRowlength > 1 && multiplierColumnLength > 1 && multiplicandRowLength == multiplierRowlength && multiplicandColumnLength == multiplierColumnLength) {   /* multiplier为矩阵-矩阵维度必须一致（行数、列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = multiplicand[row][column] * multiplier[row][column];
                    }
                }
            } else if (multiplierRowlength == 1 && multiplierColumnLength == 1) {                                                    /* multiplier单一数值时*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = multiplicand[row][column] * multiplier[0][0];
                    }
                }
            } else {                                                                                                           /* 矩阵维度不一致*/
                return null;
            }
        } else if (multiplicandRowLength == 1 && multiplicandColumnLength == 1) {                                                      /* multiplicand为单一数值时*/
            for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                    result[row][column] = multiplicand[0][0] * multiplier[row][column];
                }
            }
        }
        return result;


    }


    /**
     * 乘法
     *
     * @param multiplicand 被乘数
     * @param multiplier   乘数
     * @return
     */
    public static double[] multiplication(double[] multiplicand, double multiplier) {

        int multiplicandLength = multiplicand.length;
        double[] result = new double[multiplicandLength]; /* 减法返回值*/

        for (int column = 0; column < multiplicandLength; column++) {   //循环遍历列
            result[column] = multiplicand[column] * multiplier;
        }

        return result;


    }


    /**
     * 除法
     *
     * @param dividend 被除数
     * @param divisor  除数
     * @return
     */
    public static double[][] divide(double[][] dividend, double[][] divisor) {

        int dividendRowLength = dividend.length;
        int dividendColumnLength = dividend[0].length;
        int divisorRowLength = divisor.length;
        int divisorColumnLength = divisor[0].length;
        int maxRowLength = divisorRowLength > dividendRowLength ? divisorRowLength : dividendRowLength; /* 获取行数最大值*/
        int maxColumnLength = divisorColumnLength > dividendColumnLength ? divisorColumnLength : dividendColumnLength; /* 获取列数最大值*/
        double[][] result = new double[maxRowLength][maxColumnLength]; /* 结果值*/

        /* 判断数据格式*/
        if (dividendRowLength == 1 && dividendColumnLength > 1) {                                                           /* dividend为行向量 */
            if (divisorRowLength == 1 && divisorColumnLength > 1 && dividendColumnLength == divisorColumnLength) {              /* divisor为行向量-矩阵维度必须一致（列数一致）*/
                for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                    result[0][column] = dividend[0][column] / divisor[0][column];
                }
            } else if (divisorRowLength > 1 && divisorColumnLength == 1) {                                                      /* divisor为列向量*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = dividend[0][column] / divisor[row][0];
                    }
                }
            } else if (divisorRowLength > 1 && divisorColumnLength > 1 && dividendColumnLength == divisorColumnLength) {      /* divisor为矩阵-矩阵维度必须一致（列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = dividend[0][column] / divisor[row][column];
                    }
                }
            } else if (divisorRowLength == 1 && divisorColumnLength == 1) {                                                    /* divisor单一数值时*/
                for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                    result[0][column] = dividend[0][column] / divisor[0][0];
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }
        } else if (dividendRowLength > 1 && dividendColumnLength == 1) {                                                  /* dividend为列向量*/
            if (divisorRowLength == 1 && divisorColumnLength > 1) {                                                             /* divisor为行向量*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = dividend[row][0] / divisor[0][column];
                    }
                }
            } else if (divisorRowLength > 1 && divisorColumnLength == 1 && dividendRowLength == divisorRowLength) {            /* divisor为列向量-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {
                    result[row][0] = dividend[row][0] / divisor[row][0];
                }
            } else if (divisorRowLength > 1 && divisorColumnLength > 1 && dividendRowLength == divisorRowLength) {             /* divisor为矩阵-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = dividend[row][0] / divisor[0][column];
                    }
                }
            } else if (divisorRowLength == 1 && divisorColumnLength == 1) {                                                    /* divisor单一数值时*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    result[row][0] = dividend[0][0] / divisor[0][0];
                }
            } else {                                                                                                       /* 矩阵维度不一致*/
                return null;
            }

        } else if (dividendRowLength > 1 && dividendColumnLength > 1) {                                                    /* dividend为矩阵*/
            if (divisorRowLength == 1 && divisorColumnLength > 1 && dividendColumnLength == divisorColumnLength) {             /* divisor为行向量-矩阵维度必须一致（列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = dividend[row][column] / divisor[0][column];
                    }
                }
            } else if (divisorRowLength > 1 && divisorColumnLength == 1 && dividendRowLength == divisorRowLength) {             /* divisor为列向量-矩阵维度必须一致（行数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = dividend[row][column] / divisor[row][0];
                    }
                }
            } else if (divisorRowLength > 1 && divisorColumnLength > 1 && dividendRowLength == divisorRowLength && dividendColumnLength == divisorColumnLength) {   /* divisor为矩阵-矩阵维度必须一致（行数、列数一致）*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = dividend[row][column] / divisor[row][column];
                    }
                }
            } else if (divisorRowLength == 1 && divisorColumnLength == 1) {                                                    /* divisor单一数值时*/
                for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                    for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                        result[row][column] = dividend[row][column] / divisor[0][0];
                    }
                }
            } else {                                                                                                           /* 矩阵维度不一致*/
                return null;
            }
        } else if (dividendRowLength == 1 && dividendColumnLength == 1) {                                                      /* dividend为单一数值时*/
            for (int row = 0; row < maxRowLength; row++) {   //循环遍历行
                for (int column = 0; column < maxColumnLength; column++) {   //循环遍历列
                    result[row][column] = dividend[0][0] / divisor[row][column];
                }
            }
        }
        return result;

    }

    /**
     * 除法
     *
     * @param dividend 被除数
     * @param divisor  除数
     * @return
     */
    public static double[] divide(double[] dividend, double[] divisor) {

        int dividendLength = dividend.length;
        int divisorLength = divisor.length;
        int maxLength = divisorLength > dividendLength ? divisorLength : dividendLength; /* 获取行数最大值*/
        double[] result = new double[maxLength]; /* 结果值*/

        for (int row = 0; row < maxLength; row++) {
//            result[row] = dividend[row] / divisor[row];
            result[row] = MathUtil.divideDouble(dividend[row], divisor[row], 4);
        }

        return result;

    }


    /**
     * 除法
     *
     * @param dividend 被除数
     * @param divisor  除数
     * @return
     */
    public static double[] divide(double[] dividend, double divisor) {

        int dividendLength = dividend.length;
        double[] result = new double[dividendLength]; /* 结果值*/

        for (int i = 0; i < dividendLength; i++) {
            result[i] = dividend[i] / divisor;
        }

        return result;

    }


    /**
     * 求标准偏差   默认每列求一个标准偏差
     *
     * @param value
     * @return
     */
    public static double[][] std(double[][] value) {

        int rowLength = value.length;   /* 行数*/
        int columnLength = value[0].length; /* 列数*/
        double[][] result = new double[rowLength][columnLength]; /* 返回值*/

        if (rowLength == 1 && columnLength > 1) {                 /* 为行向量时*/
            /* 按照行分*/
            result = new double[1][1];
            double average = mean(value, 0)[0][0]; /* 获取行的平均值*/
            double d = 0;
            for (int column = 0; column < columnLength; column++) {
                d += (value[0][column] - average) * (value[0][column] - average);
            }
            result[0][0] = Math.sqrt(d / (columnLength - 1));  /* 标准方差计算公式*/
        } else if (rowLength > 1 && columnLength == 1) {        /* 为列向量时*/
            /* 按照列分*/
            result = new double[1][1];
            double average = mean(value, 0)[0][0]; /* 获取列平均值*/
            double d = 0;
            for (int row = 0; row < rowLength; row++) {
                d += (value[row][0] - average) * (value[row][0] - average);
            }
            result[0][0] = Math.sqrt(d / (rowLength - 1));  /* 标准方差计算公式*/
        } else if (rowLength > 1 && columnLength > 1) {        /* 为矩阵时*/
            /* 按照列分*/
            result = new double[1][columnLength];
            double[][] average = mean(value, 1);  /* 按照每列求一个平均值*/
            for (int column = 0; column < columnLength; column++) {   /* 循环遍历各列，求每列的标准偏差*/
                double d = 0;
                for (int row = 0; row < rowLength; row++) {
                    d += (value[row][column] - average[0][column]) * (value[row][column] - average[0][column]);
                }
                result[0][column] = Math.sqrt(d / (rowLength - 1));  /* 标准方差计算公式*/
            }
        }
        return result;

    }


    /**
     * 求标准偏差   默认每列求一个标准偏差
     *
     * @param value
     * @return
     */
    public static double std(double[] value) {

        int valueLength = value.length;   /* 数量*/
        double result; /* 返回值*/

        double average = mean(value); /* 获取平均值*/
        double d = 0;
        for (int column = 0; column < valueLength; column++) {
            d += (value[column] - average) * (value[column] - average);
        }
        result = Math.sqrt(d / (valueLength - 1));  /* 标准方差计算公式*/
        return result;

    }


    /**
     * 求标准偏差（当数据为行向量或列向量时）   默认格式std(value, 0, 1)
     *
     * @param value 向量或矩阵
     * @param flag  0：求标准偏差时除以n-1; 1:求标准偏差时除以n
     * @param dim   1:按照列分（每列求一个值）   2：按照行分（每行求一个值）
     * @return
     */
    public static double[][] std(double[][] value, int flag, int dim) {

        int rowLength = value.length;   /* 行数*/
        int columnLength = value[0].length; /* 列数*/
        double[][] result = new double[rowLength][columnLength]; /* 返回值*/

        if (rowLength == 1 && columnLength > 1) {                 /* 为行向量时*/
            if (dim == 1) {                                            /* 按照列分*/
                result = new double[1][columnLength];
                for (int column = 0; column < columnLength; column++) {
                    result[0][column] = 0;
                }
            } else if (dim == 2) {                                      /* 按照行分*/
                result = new double[1][1];
                double average = mean(value, 0)[0][0]; /* 获取行的平均值*/
                double d = 0;
                for (int column = 0; column < columnLength; column++) {
                    d += (value[0][column] - average) * (value[0][column] - average);
//                    d = MathUtil.addDouble(d, Math.pow(MathUtil.subtractDouble(value[0][column], average), 2));
                }
                result[0][0] = Math.sqrt(d / (flag == 0 ? columnLength - 1 : columnLength));  /* 标准方差计算公式*/
//                result[0][0] = Math.sqrt(MathUtil.divideDouble(d, (flag == 0 ? columnLength - 1 : columnLength)));
            } else {
                return null;
            }
        } else if (rowLength > 1 && columnLength == 1) {        /* 为列向量时*/
            if (dim == 1) {                                            /* 按照列分*/
                result = new double[1][1];
                double average = mean(value, 0)[0][0]; /* 获取列平均值*/
                double d = 0;
                for (int row = 0; row < rowLength; row++) {
//                    d += (value[row][0] - average) * (value[row][0] - average);
                    d = MathUtil.addDouble(d, Math.pow(MathUtil.subtractDouble(value[row][0], average), 2));
                }
//                result[0][0] = Math.sqrt(d / (flag == 0 ? rowLength - 1 : rowLength));  /* 标准方差计算公式*/
                double c = MathUtil.divideDouble(d, rowLength - 1);
                double m = Math.sqrt(c);
                result[0][0] = Math.sqrt(MathUtil.divideDouble(d, (flag == 0 ? rowLength - 1 : rowLength)));
            } else if (dim == 2) {                                     /* 按照行分*/
                result = new double[3][1];
                for (int row = 0; row < rowLength; row++) {
                    result[row][0] = 0;
                }
            }
        } else if (rowLength > 1 && columnLength > 1) {
            if (dim == 1) {          /* 按照列分*/
                result = new double[1][columnLength];
                double[][] average = mean(value, 1);  /* 按照每列求一个平均值*/
                for (int column = 0; column < columnLength; column++) {   /* 循环遍历各列，求每列的标准偏差*/
                    double d = 0;
                    for (int row = 0; row < rowLength; row++) {
//                        d += (value[row][column] - average[0][column]) * (value[row][column] - average[0][column]);
                        d = MathUtil.addDouble(d, Math.pow(MathUtil.subtractDouble(value[row][column], average[0][column]), 2));
                    }
//                    result[0][column] = Math.sqrt(d / (flag == 0 ? rowLength - 1 : rowLength));  /* 标准方差计算公式*/
                    result[0][column] = Math.sqrt(MathUtil.divideDouble(d, (flag == 0 ? rowLength - 1 : rowLength)));
                }
            } else if (dim == 2) {   /* 按照行分*/
                result = new double[rowLength][1];
                double[][] average = mean(value, 2);  /* 按照每行求平一个均值*/
                for (int row = 0; row < rowLength; row++) {
                    double d = 0;
                    for (int column = 0; column < columnLength; column++) {
//                        d += (value[row][column] - average[row][0]) * (value[row][column] - average[row][0]);
                        d = MathUtil.addDouble(d, Math.pow(MathUtil.subtractDouble(value[row][column], average[row][0]), 2));
                    }
//                    result[row][0] = Math.sqrt(d / (flag == 0 ? columnLength - 1 : columnLength));  /* 标准方差计算公式*/
                    result[row][0] = Math.sqrt(MathUtil.divideDouble(d, (flag == 0 ? columnLength - 1 : columnLength)));
                }
            }
        }
        return result;

    }


    /**
     * 获取矩阵数组指定列的列向量
     *
     * @param column
     * @return
     */
    public static double[][] column(double[][] value, int column) {

        int rowLength = value.length;
        double[][] result = new double[rowLength][1];  /* 返回值*/
        for (int row = 0; row < rowLength; row++) {
            result[row][0] = value[row][column];
        }
        return result;

    }


    /**
     * 获取矩阵数组指定行的行向量
     *
     * @param row
     * @return 二维数据
     */
    public static double[][] row(double[][] value, int row) {

        int columnLength = value[0].length;
        double[][] result = new double[1][columnLength];  /* 返回值*/
        for (int column = 0; column < columnLength; column++) {
            result[0][column] = value[row][column];
        }
        return result;

    }


    /**
     * 获取矩阵数组指定行的行向量
     *
     * @param row
     * @return 一维数据
     */
    public static double[] singleRow(double[][] value, int row) {

        int columnLength = value[0].length;
        double[] result = new double[columnLength];  /* 返回值*/
        for (int column = 0; column < columnLength; column++) {
            result[column] = value[row][column];
        }
        return result;

    }


    /**
     * 将矩阵组合成列向量   value(:)
     *
     * @param value
     * @return
     */
    public static double[][] matrixToColumn(double[][] value) {

        int rowLength = value.length;
        int columnLength = value[0].length;
        double[][] result = new double[rowLength * columnLength][1];
        for (int column = 0; column < columnLength; column++) {    /* 循环遍历每列组合成列向量*/
            for (int row = 0; row < rowLength; row++) {
                result[column * rowLength + row][0] = value[row][column];
            }
        }
        return result;

    }


    /**
     * 一维数据版本：将矩阵转化为列向量 并获取指定区间的元素  value(:)中start到stop
     *
     * @param value
     * @param start
     * @param stop
     * @return
     */
    public static double[] matrixStartStop(double[] value, int start, int stop) {

        double[] result = new double[stop - start + 1];                  /* 返回值*/
        int j = 0;
        for (int i = start; i <= stop; i++) {
            result[j++] = value[i];
        }
        return result;

    }


    /**
     * 二维数据版本：将矩阵转化为列向量 并获取指定区间的元素  value(:)中start到stop
     *
     * @param value
     * @param start
     * @param stop
     * @return
     */
    public static double[][] matrixStartStop(double[][] value, int start, int stop) {

        double[][] newValue = matrixToColumn(value);             /* 矩阵转化为列向量*/
        double[][] result = new double[stop - start + 1][1];                  /* 返回值*/
        int j = 0;
        for (int row = start; row <= stop; row++) {
            result[j++][0] = newValue[row][0];
        }
        return result;

    }


    /**
     * 将矩阵转化为列向量 并获取指定位置的元素  value(spot)
     *
     * @param value
     * @param spot
     * @return
     */
    public static double matrixSpot(double[][] value, int spot) {

        double[][] newValue = matrixToColumn(value);             /* 矩阵转化为列向量*/
        return newValue[spot][0];                  /* 返回值*/

    }


    /**
     * 获取数组中指定位置的数据
     *
     * @param value
     * @param spot  数值默认从1开始，需要减1
     * @return
     */
    public static double[] arraySpot(double[] value, double[] spot) {

        double[] result = new double[spot.length];
        for (int i = 0; i < spot.length; i++) {
            result[i] = value[(int) spot[i] - 1];
        }
        return result;

    }


    /**
     * 求中间值
     *
     * @param value
     * @param dim   1：每列返回一个值，为该列从大到小排列的中间值    2：每行返回一个值，为该行从大到小排列的中间值
     * @return
     */
    public static double median(double[] value, int dim) {

        int valueLength = value.length;
        double result;
        value = sort(value);
        if (valueLength % 2 == 0) {
            result = MathUtil.divideDouble(MathUtil.addDouble(value[valueLength / 2 - 1], value[valueLength / 2]), 2);
        } else {
            result = value[valueLength / 2];
        }
        return result;

    }

    /**
     * 求矩阵的中间值
     *
     * @param value
     * @param dim   1：每列返回一个值，为该列从大到小排列的中间值    2：每行返回一个值，为该行从大到小排列的中间值
     * @return
     */
    public static double[][] median(double[][] value, int dim) {

        int rowLength = value.length;
        int columnLength = value[0].length;
        double[][] result;
        result = sort(value, dim);
        if (dim == 1) {             /* 每列求中间值*/
            result = new double[1][columnLength];
            for (int column = 0; column < columnLength; column++) {
                if (rowLength % 2 == 0) {
                    result[0][column] = (value[rowLength / 2][column] + value[rowLength / 2 + 1][column]) / (double) 2;
                } else {
                    result[0][column] = value[rowLength / 2 + 1][column];
                }
            }
        } else if (dim == 2) {     /* 每行求中间值*/
            result = new double[rowLength][1];
            for (int row = 0; row < rowLength; row++) {
                if (columnLength % 2 == 0) {
                    result[row][0] = (value[row][columnLength / 2] + value[row][columnLength / 2 + 1]) / (double) 2;
                } else {
                    result[row][0] = value[row][columnLength / 2 + 1];
                }
            }
        }

        return result;

    }

    /**
     * 一维数据从大到小进行排序
     *
     * @param value
     * @return
     */
    public static double[] sort(double[] value) {

        int valueLength = value.length;
        double[] result = new double[valueLength];
        Arrays.sort(value);
        int j = 0;
        for (int i = valueLength - 1; i >= 0; i--) {
            result[j++] = value[i];
        }
        return result;

    }

    /**
     * 矩阵进行排序
     *
     * @param value
     * @param dim   1：每列按照从大到小排序   2：每行按照从大到小排序
     * @return
     */
    public static double[][] sort(double[][] value, int dim) {

        int rowLength = value.length;
        int columnLength = value[0].length;
        double[][] result = new double[rowLength][columnLength];
        if (dim == 1) {                                           /* 按列排序*/
            for (int column = 0; column < columnLength; column++) {          /* 遍历列*/
                double[] temporary = new double[rowLength];
                for (int row = 0; row < rowLength; row++) {
                    temporary[row] = value[row][0];                       /* 获取列数组*/
                }
                Arrays.sort(temporary);                                 /* 排序（从小到大）*/
                int j = 0;
                for (int i = temporary.length - 1; i >= 0; i--) {
                    result[j++][column] = temporary[i];                /* 将排序好的数组赋值给结果*/
                }
            }
        } else if (dim == 2) {                                   /* 按行排序*/
            for (int row = 0; row < rowLength; row++) {
                double[] temporary = new double[columnLength];
                for (int column = 0; column < columnLength; column++) {
                    temporary[column] = value[0][column];
                }
                Arrays.sort(temporary);
                int j = 0;
                for (int i = temporary.length; i >= 0; i--) {
                    result[row][j++] = temporary[i];
                }
            }
        }
        return result;

    }

    /**
     * 矩阵进行排序
     *
     * @param value
     * @param dim   1：每列按照从小到大排序   2：每行按照从小到大排序
     * @return
     */
    public static double[][] ascSort(double[][] value, int dim) {

        int rowLength = value.length;
        int columnLength = value[0].length;
        double[][] result = new double[rowLength][columnLength];
        if (dim == 1) {                                           /* 按列排序*/
            for (int column = 0; column < columnLength; column++) {          /* 遍历列*/
                double[] temporary = new double[rowLength];
                for (int row = 0; row < rowLength; row++) {
                    temporary[row] = value[row][0];                       /* 获取列数组*/
                }
                Arrays.sort(temporary);                                 /* 排序（从小到大）*/
                int j = 0;
                for (int i = 0; i < temporary.length; i++) {
                    result[j++][column] = temporary[i];                /* 将排序好的数组赋值给结果*/
                }
            }
        } else if (dim == 2) {                                   /* 按行排序*/
            for (int row = 0; row < rowLength; row++) {
                double[] temporary = new double[columnLength];
                for (int column = 0; column < columnLength; column++) {
                    temporary[column] = value[0][column];
                }
                Arrays.sort(temporary);
                int j = 0;
                for (int i = 0; i < temporary.length; i++) {
                    result[row][j++] = temporary[i];
                }
            }
        }
        return result;

    }

    /**
     * 中括号 -暂时只支持两个行向量组合成一个行向量
     *
     * @param value1
     * @param value2
     * @return
     */
    public static double[] squareBrackets(double[] value1, double[] value2) {

        int value1Length = value1.length;
        int value2Length = value2.length;
        double[] result = new double[value1Length + value2Length];
        for (int i = 0; i < value1Length; i++) {
            result[i] = value1[i];
        }
        for (int i = 0; i < value2Length; i++) {
            result[i + value1Length] = value2[i];
        }
        return result;

    }

    /**
     * 中括号 -暂时只支持两个列向量组合
     *
     * @param value1
     * @param value2
     * @return
     */
    public static double[][] squareBrackets(double[][] value1, double[][] value2) {

        int value1RowLength = value1.length;
        int value2RowLength = value2.length;
        double[][] result = new double[(value1RowLength + value2RowLength)][1];
        for (int row = 0; row < value1RowLength; row++) {
            result[row][0] = value1[row][0];
        }
        for (int row = 0; row < value2RowLength; row++) {
            result[value1RowLength + row][0] = value2[row][0];
        }
        return result;

    }


    /**
     * 获取指定的行
     *
     * @param value 矩阵数组
     * @param index 行索引（从0开始）
     * @return
     */
    public static double[] getIndexRow(double[][] value, int index) {

        int valueRowLength = value[0].length;
        double[] result = new double[valueRowLength];
        for (int i = 0; i < valueRowLength; i++) {
            result[i] = value[index][i];
        }
        return result;

    }


    /**
     * 获取百分位数
     *
     * @param x 暂时-只支持列向量
     * @param p 25-Q1, 50-Q2, 75-Q3
     * @return
     */
    public static double prctile(double[][] x, int p) {

        int columnLength = x[0].length;
        x = ascSort(x, 2);
        double result = 0; /* 结果值*/
        if (columnLength == 1) {
            result = x[0][0];
        } else if (columnLength == 2) {
            if (p == 25) {
                result = x[0][0];
            } else if (p == 75) {
                result = x[0][1];
            } else if (p == 50) {
                result = MathUtil.divideDouble(MathUtil.addDouble(x[0][0], x[0][1]), 2);
            }
        } else if (columnLength > 2) {
            if (p == 25 || p == 75) {             /* 1/4、3/4 位置*/
                double position;
                if (p == 25) {
                    /* Q1所在的位置*/
                    position = (columnLength + 1) / (double) 4;
                } else {
                    /* Q3所在的位置*/
                    position = (3 * (columnLength + 1)) / (double) 4;
                }
                double frontPosition = Math.floor(position) - 1;  /* 前一个位置*/
                double backPosition = Math.ceil(position) - 1;    /* 后一个位置*/
                if (p == 25) {
                    result = (double) (0.25 * x[0][(int) frontPosition] + 0.75 * x[0][(int) backPosition]);
                } else {
                    result = (double) (0.75 * x[0][(int) frontPosition] + 0.25 * x[0][(int) backPosition]);
                }

            } else if (p == 50) {                 /* 中位*/
                if (columnLength % 2 == 0) {       /* 数组数量为偶数时*/
                    result = (x[0][columnLength / 2 - 1] + x[0][columnLength / 2 + 1]) / 2;
                } else {                     /* 数组数量为奇数时*/
                    result = x[0][columnLength / 2];
                }
            }
        }

        return result;

    }


    /**
     * 计算日期相差的秒数（暂时只支持列向量）
     *
     * @param dates
     * @return
     */
    public static double[][] dateDiff(String[][] dates) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int rowLength = dates.length;
        int columnLength = dates[0].length;
        double[][] result = new double[rowLength - 1][1];

        try {
            for (int row = 0; row < rowLength - 1; row++) {
                Date date = simpleDateFormat.parse(dates[row][0]);
                Date afterDate = simpleDateFormat.parse(dates[row + 1][0]);
                result[row][0] = (afterDate.getTime() - date.getTime()) / 1000;    /* 获取相差的秒数*/
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;

    }

    /**
     * 计算日期相差的天数（一天就以1为单位）   返回结果最前边加个0
     *
     * @param dates
     * @return
     */
    public static double[] dateDiff(String[] dates) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int length = dates.length;
        double[] result = new double[length];

        try {
            result[0] = 0;
            for (int i = 0; i < length - 1; i++) {
                Date date = simpleDateFormat.parse(dates[i]);
                Date afterDate = simpleDateFormat.parse(dates[i + 1]);
                result[i + 1] = MathUtil.divideDouble(MathUtil.subtractDouble(afterDate.getTime(), date.getTime()), 24 * 60 * 60 * 1000, 4);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;

    }

    /**
     * 一次函数线性回归y = ax + b;
     *
     * @param x
     * @param y
     * @return 返回 a、b
     */
    public static double[] linearRegression(double[] x, double[] y) {

        double xSum = 0, ySum = 0;   /* x的多项和、y的多项和*/
        for (int i = 0; i < x.length; i++) {
            xSum = MathUtil.addDouble(xSum, x[i]);
            ySum = MathUtil.addDouble(ySum, y[i]);
        }
        double xMean = MathUtil.divideDouble(xSum, x.length);    /* 求平均值*/
        double yMean = MathUtil.divideDouble(ySum, y.length);    /* 求平均值*/
        double num = 0;            /*多项式和【(x-x的均值)*(y-y的均值)】*/
        double den = 0;           /*多项式和【(x-x的均值)*(x-x的均值)】*/
        for (int i = 0; i < x.length; i++) {
            num = MathUtil.addDouble(num, MathUtil.multiplyDouble(MathUtil.subtractDouble(x[i], xMean), MathUtil.subtractDouble(y[i], yMean)));
            den = MathUtil.addDouble(den, MathUtil.multiplyDouble(MathUtil.subtractDouble(x[i], xMean), MathUtil.subtractDouble(x[i], xMean)));
        }
        double a = MathUtil.divideDouble(num, den, 4);
        double b = MathUtil.subtractDouble(yMean, MathUtil.multiplyDouble(a, xMean));
        double[] result = new double[]{a, b};
        return result;

    }


    /**
     * 统计数组的值、频数、频率
     *
     * @param value
     * @return 返回多行三列的数据 每列依次是元素值、频数、频率（100代表100%）
     */
    public static double[][] tabulate(int[] value) {

        Arrays.sort(value);
        double maxValue = MatlabUtil.max(value);   /* 获取数组最大值*/
        double[][] result = new double[(int) maxValue][3];   /* 定义返回结果*/
        int valueLength = value.length;  /* 获取数组长度*/
        for (int i = 1; ; i++) {
            int count = 0;  /* 数组出现的频数*/
            for (int j = 0; j < valueLength; j++) {
                if (i == value[j]) {
                    count++;
                } else {
                    if (count > 0) break;
                }
            }
            result[i - 1][0] = i;   /* value数据值*/
            if (count > 0) {        /* value数据出现时*/
                result[i - 1][1] = count;                                             /* value数据出现的频次*/
                result[i - 1][2] = MathUtil.multiplyDouble(MathUtil.divideDouble(count, valueLength), 100);
            } else {               /* value数据未出现时*/
                result[i - 1][1] = 0;
                result[i - 1][2] = 0;
            }
            if (i == maxValue) {
                break;        /* 统计结束跳出做外层循环*/
            }
        }
        return result;

    }


    /**
     * 统计数组的值、频数、频率
     *
     * @param value
     * @return 返回多行三列的数据 每列依次是元素值、频数、频率（100代表100%）
     */
    public static double[][] tabulate(double[] value) {

        Arrays.sort(value);
        double maxValue = MatlabUtil.max(value);   /* 获取数组最大值*/
        double[][] result = new double[(int) maxValue][3];   /* 定义返回结果*/
        int valueLength = value.length;  /* 获取数组长度*/
        for (int i = 1; ; i++) {
            int count = 0;  /* 数组出现的频数*/
            for (int j = 0; j < valueLength; j++) {
                if (i == value[j]) {
                    count++;
                } else {
                    if (count > 0) break;
                }
            }
            result[i - 1][0] = i;   /* value数据值*/
            if (count > 0) {        /* value数据出现时*/
                result[i - 1][1] = count;                                             /* value数据出现的频次*/
                result[i - 1][2] = ((double) count / (double) valueLength) * (double) 100;  /* value数据出现的频率*/
            } else {               /* value数据未出现时*/
                result[i - 1][1] = 0;
                result[i - 1][2] = 0;
            }
            if (i == maxValue) {
                break;        /* 统计结束跳出做外层循环*/
            }
        }
        return result;

    }


    /**
     * 数组去重
     *
     * @param value
     * @return
     */
    public static Integer[] distinct(int[] value) {

        List<Integer> list = new ArrayList();
        for (int i = 0; i < value.length; i++) {
            if (!list.contains(value[i])) {
                list.add(value[i]);
            }
        }
        return list.toArray(new Integer[1]);

    }


    /**
     * 获取指定的列
     *
     * @param value 矩阵
     * @param index 列索引
     * @return
     */
    public static double[] indexColumn(double[][] value, int index) {

        int rowLength = value.length;
        double[] result = new double[rowLength];
        for (int i = 0; i < rowLength; i++) {
            result[i] = value[i][index - 1];
        }
        return result;

    }


    /**
     * boxoutlier算法中的find()函数-暂不支持扩展
     *
     * @param x          列向量
     * @param condition1
     * @param condition2
     * @return
     */
    public static double[][] boxoutlierFind(double[][] x, double condition1, double condition2) {

        int rowLength = x.length;   /* 获取行数*/
        int columnLength = x[0].length;  /* 获取列数*/
        double[][] result = new double[rowLength][1];           /* 返回值(未去掉多余的长度)*/
        int resultRow = 0;
        for (int column = 0; column < columnLength; column++) {
            for (int row = 0; row < rowLength; row++) {
                double cellValue = x[row][column];
                if (cellValue < condition1 || cellValue > condition2) {
                    result[resultRow][0] = column * rowLength + row + 1;
                    resultRow++;
                }
            }
        }
        if (resultRow == 0) {
            return null;
        } else {
            double[][] newResult = new double[resultRow][1];
            for (int i = 0; i < resultRow; i++) {
                newResult[i][0] = result[i][0];
            }
            return newResult;
        }

    }


    public static int maxValueIndex(double[] value) {

        int valueLength = value.length;
        int index = 1;
        double maxValue = value[0];
        for (int i = 1; i < valueLength; i++) {
            if (value[i] > maxValue) {
                maxValue = value[i];
                index = i + 1;
            }
        }
        return index;

    }


    /**
     * reduce 模型三算法中的find函数-暂不支持扩展
     *
     * @param x
     * @param condition1 小于condition的元素位置
     * @return
     */
    public static double[][] reduceFind(double[][] x, double condition1) {

        int rowLength = x.length;
        int columnLength = x[0].length;
        double[][] result = new double[1][1];
        boolean flag = false;  /* 标识结果是否没有值  false:没有值*/
        int length = 0;

        if (rowLength > 1 && columnLength > 1) {             /* 为矩阵   返回列向量*/
            double[][] newX = matrixToColumn(x);             /* 矩阵转为列向量*/
            result = new double[newX.length][1];
            for (int row = 0; row < newX.length; row++) {
                if (newX[row][0] < condition1) {
                    result[length++][0] = row + 1;
                    flag = true;
                }
            }
            /* 去除长度多余的数据(为0的数据)*/
            double[][] temp = new double[length][1];
            for (int i = 0; i < length; i++) {
                temp[i][0] = result[i][0];
            }
            result = temp;
        } else if (rowLength > 1 && columnLength == 1) {     /* 为列向量   返回列向量*/
            result = new double[x.length][1];
            for (int row = 0; row < x.length; row++) {
                if (x[row][0] < condition1) {
                    result[length++][0] = row + 1;
                    flag = true;
                }
            }
            /* 去除长度多余的数据(为0的数据)*/
            double[][] temp = new double[length][1];
            for (int i = 0; i < length; i++) {
                temp[i][0] = result[i][0];
            }
            result = temp;
        } else if (rowLength == 1 && columnLength > 1) {     /* 为行向量   返回行向量*/
            result = new double[1][x[0].length];
            for (int column = 0; column < x[0].length; column++) {
                if (x[0][column] < condition1) {
                    result[0][length++] = column + 1;
                    flag = true;
                }
            }
            /* 去除长度多余的数据(为0的数据)*/
            double[][] temp = new double[1][length];
            for (int i = 0; i < length; i++) {
                temp[0][i] = result[0][i];
            }
            result = temp;
        }

        if (flag == false) {
            result = null;
        }
        return result;

    }

    /**
     * reduce 模型三算法中的find函数-暂不支持扩展
     *
     * @param x
     * @param condition1 小于condition的元素位置
     * @return
     */
    public static double[] reduceFind(double[] x, double condition1) {

        int xLength = x.length;
        double[] result = new double[x.length];
        boolean flag = false;  /* 标识结果是否没有值  false:没有值*/
        int length = 0;

        for (int i = 0; i < xLength; i++) {
            if (x[i] < condition1) {
                result[length++] = i + 1;
                flag = true;
            }
        }
        /* 去除长度多余的数据(为0的数据)*/
        double[] temp = new double[length];
        for (int i = 0; i < length; i++) {
            temp[i] = result[i];
        }
        result = temp;

        if (flag == false) {
            result = null;
        }
        return result;

    }


    /**
     * 一维数据：模型四算法中的find()函数   获取元素的位置
     *
     * @param x
     * @param condition1 大于等于condition1元素的位置
     * @return
     */
    public static int[] selfDischargeBigFind(double[] x, double condition1) {

        int xLength = x.length;
        int[] result = new int[xLength];
        boolean flag = false;  /* 标识结果是否没有值  false:没有值*/
        int length = 0; /* result动态长度*/

        for (int i = 0; i < xLength; i++) {
            if (x[i] >= condition1) {
                result[length++] = i + 1;
                flag = true;
            }
        }
        /* 去除长度多余的数据(为0的数据)*/
        int[] temp = new int[length];
        for (int i = 0; i < length; i++) {
            temp[i] = result[i];
        }
        result = temp;

        if (flag == false) {
            result = null;
        }
        return result;

    }


    /**
     * 二维数据：模型四算法中的find()函数   获取元素的位置
     *
     * @param x
     * @param condition1 大于等于condition1元素的位置
     * @return
     */
    public static int[][] selfDischargeBigFind(double[][] x, double condition1) {

        int rowLength = x.length;
        int columnLength = x[0].length;
        int[][] result = new int[1][1];
        boolean flag = false;  /* 标识结果是否没有值  false:没有值*/
        int length = 0; /* result动态长度*/

        if (rowLength > 1 && columnLength > 1) {             /* 为矩阵   返回列向量*/
            double[][] newX = matrixToColumn(x);             /* 矩阵转为列向量*/
            result = new int[newX.length][1];
            for (int row = 0; row < newX.length; row++) {
                if (newX[row][0] >= condition1) {
                    result[length++][0] = row + 1;
                    flag = true;
                }
            }
            /* 去除长度多余的数据(为0的数据)*/
            int[][] temp = new int[length][1];
            for (int i = 0; i < length; i++) {
                temp[i][0] = result[i][0];
            }
            result = temp;
        } else if (rowLength > 1 && columnLength == 1) {     /* 为列向量   返回列向量*/
            result = new int[x.length][1];
            for (int row = 0; row < x.length; row++) {
                if (x[row][0] >= condition1) {
                    result[length++][0] = row + 1;
                    flag = true;
                }
            }
            /* 去除长度多余的数据(为0的数据)*/
            int[][] temp = new int[length][1];
            for (int i = 0; i < length; i++) {
                temp[i][0] = result[i][0];
            }
            result = temp;
        } else if (rowLength == 1 && columnLength > 1) {     /* 为行向量   返回行向量*/
            result = new int[1][x[0].length];
            for (int column = 0; column < x[0].length; column++) {
                if (x[0][column] >= condition1) {
                    result[0][length++] = column + 1;
                    flag = true;
                }
            }
            /* 去除长度多余的数据(为0的数据)*/
            int[][] temp = new int[1][length];
            for (int i = 0; i < length; i++) {
                temp[0][i] = result[0][i];
            }
            result = temp;
        }
        if (flag == false) {
            result = null;
        }
        return result;

    }


    /**
     * 将数据按照从小到大排序 输出数据所在位置的索引index
     *
     * @param arr
     * @param desc
     * @return
     */
    public static int[] getIndexBySort(double[] arr, boolean desc) {

        double temp;
        int index;
        int k = arr.length;
        int[] Index = new int[k];
        for (int i = 0; i < k; i++) {
            Index[i] = i + 1;
        }

        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr.length - i - 1; j++) {
                if (desc) {
                    if (arr[j] < arr[j + 1]) {
                        temp = arr[j];
                        arr[j] = arr[j + 1];
                        arr[j + 1] = temp;

                        index = Index[j];
                        Index[j] = Index[j + 1];
                        Index[j + 1] = index;
                    }
                } else {
                    if (arr[j] > arr[j + 1]) {
                        temp = arr[j];
                        arr[j] = arr[j + 1];
                        arr[j + 1] = temp;

                        index = Index[j];
                        Index[j] = Index[j + 1];
                        Index[j + 1] = index;
                    }
                }
            }
        }
        return Index;


    }


}
