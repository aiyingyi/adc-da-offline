package com.adc.da.functions;

import ch.ethz.ssh2.Connection;
import com.adc.da.bean.EventInfo;
import com.adc.da.util.ShellUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Properties;

/**
 * 执行shell脚本的sink
 *
 * @param <T> stream的类型
 */
public class ShellRichSink<T> extends RichSinkFunction<T> {

    // shell环境配置以及脚本执行路径
    public Properties shellConfig = null;
    public Connection conn = null;


    public ShellRichSink(Properties shellConfig) {
        this.shellConfig = shellConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = ShellUtil.getConnection(shellConfig.getProperty("userName"), shellConfig.getProperty("passWord"), shellConfig.getProperty("ip"), Integer.parseInt(shellConfig.getProperty("port")));
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * 调用的时候重写
     */
    @Override
    public void invoke(T value, Context context) throws Exception {
    }
}
