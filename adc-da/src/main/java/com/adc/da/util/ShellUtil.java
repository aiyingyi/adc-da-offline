package com.adc.da.util;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * 远程调用shell脚本
 */

public class ShellUtil {


    private static String charset = Charset.defaultCharset().toString();

    private static final int TIME_OUT = 1000 * 5 * 60;

    /**
     * 登录远程主机
     *
     * @return
     * @throws IOException
     */
    public static Connection getConnection(String userName, String passWord, String ip, int port) throws IOException {

        Connection conn = new Connection(ip, port);
        conn.connect();
        if (conn.authenticateWithPassword(userName, passWord)) {
            return conn;
        } else
            return null;
    }

    /**
     * 执行脚本
     *
     * @param cmd
     * @return
     * @throws Exception
     */
    public static int exec(Connection conn, String cmd) throws Exception {
        InputStream stdOut = null;
        InputStream stdErr = null;
        String outStr = "";
        String outErr = "";
        int ret = -1;
        Session session = null;
        try {
            if (conn != null) {
                session = conn.openSession();
                // 执行脚本
                session.execCommand(cmd);

                stdOut = new StreamGobbler(session.getStdout());
                outStr = processStream(stdOut, charset);
                System.out.println("outStr" + outStr);
                stdErr = new StreamGobbler(session.getStderr());
                outErr = processStream(stdErr, charset);
                System.out.println("outErr" + outErr);
                session.waitForCondition(ChannelCondition.EXIT_STATUS, TIME_OUT);
                ret = session.getExitStatus();
            } else {
                throw new Exception("登录远程机器失败");
            }
        } finally {
            if (session != null)
                session.close();
            if (stdOut != null)
                stdOut.close();
            if (stdErr != null)
                stdErr.close();
        }
        return ret;
    }

    /**
     * @param in
     * @param charset
     * @return
     * @throws Exception
     */

    private static String processStream(InputStream in, String charset) throws Exception {
        byte[] buf = new byte[1024];
        StringBuilder sb = new StringBuilder();
        while (in.read(buf) != -1) {
            sb.append(new String(buf, charset));
        }
        return sb.toString();
    }

}
