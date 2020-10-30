package com.adc.da.util;

import java.math.BigDecimal;

/**
 * 解决double类型计算精度丢失问题
 */
public class MathUtil {

    /**
     * 加法运算
     * @param number1
     * @param number2
     * @return
     */
    public static double addDouble(double number1 , double number2) {
        BigDecimal bigDecimal1 = new BigDecimal(String.valueOf(number1));
        BigDecimal bigDecimal2 = new BigDecimal(String.valueOf(number2));
        return bigDecimal1.add(bigDecimal2).doubleValue();
    }

    /**
     * 减法运算
     * @param number1
     * @param number2
     * @return
     */
    public static double subtractDouble(double number1, double number2) {
        BigDecimal bigDecimal1 = new BigDecimal(String.valueOf(number1));
        BigDecimal bigDecimal2 = new BigDecimal(String.valueOf(number2));
        return bigDecimal1.subtract(bigDecimal2).doubleValue();
    }

    /**
     * 乘法运算
     * @param number1
     * @param number2
     * @return
     */
    public static double multiplyDouble(double number1, double number2) {
        BigDecimal bigDecimal1 = new BigDecimal(String.valueOf(number1));
        BigDecimal bigDecimal2 = new BigDecimal(String.valueOf(number2));
        return bigDecimal1.multiply(bigDecimal2).doubleValue();
    }

    /**
     * 除法运算
     * @param num
     * @param total
     * @return
     */
    public static double divideDouble(double num, double total) {
        BigDecimal bigDecimal1 = new BigDecimal(String.valueOf(num));
        BigDecimal bigDecimal2 = new BigDecimal(String.valueOf(total));
        return bigDecimal1.divide(bigDecimal2, 10, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * 除法运算
     * @param num
     * @param total
     * @return
     */
    public static double divideDouble(double num, double total, int scale) {
        BigDecimal bigDecimal1 = new BigDecimal(String.valueOf(num));
        BigDecimal bigDecimal2 = new BigDecimal(String.valueOf(total));
        return bigDecimal1.divide(bigDecimal2, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

}
