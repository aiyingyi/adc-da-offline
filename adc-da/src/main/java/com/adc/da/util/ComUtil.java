package com.adc.da.util;

import com.adc.da.function.ChargeStyleElectricityFrequency;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 通用工具类
 */
public class ComUtil {

    // 加载配置文件
    public static Properties loadProperties(String configPath) {
        Properties properties = new Properties();
        InputStream in = null;
        try {
            in = ComUtil.class.getClassLoader().getResourceAsStream(configPath);
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return properties;
    }


    public static void main(String[] args) {

    }
}
