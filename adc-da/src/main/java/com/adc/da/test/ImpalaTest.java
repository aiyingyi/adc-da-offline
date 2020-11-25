package com.adc.da.test;


import java.sql.*;

public class ImpalaTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        String driver = "com.cloudera.impala.jdbc41.Driver";
        //String driver = "com.cloudera.hive.jdbc41.HS2Driver";
        //String url = "jdbc:impala://10.40.2.103:21050/default;UseSasl=0;AuthMech=3;UID=impala;PWD=";
        String url = "jdbc:impala://192.168.11.32:21050;";
        String username = "hive";
        String password = "hive";
        Connection connection = null;
        Class.forName(driver);



        connection = DriverManager.getConnection(url, username, password);

        String sql0 = "WITH charge_data  AS\n" +
                "         (SELECT get_json_object(`data`, \'$.vin\') vin,\n" +
                "                 unix_timestamp(get_json_object(`data`, \'$.msgTime\')) msgTime,\n" +
                "                 cast(get_json_object(`data`, \'$.totalCurrent\') AS DOUBLE) totalCurrent,\n" +
                "                 get_json_object(`data`, \'$.chargeStatus\') chargeStatus\n" +
                "          FROM warningplatform.ods_preprocess_vehicle_data\n" +
                "          WHERE dt >= \'2018-05-01\'\n" +
                "            AND dt <= \'2018-05-01\'\n" +
                "            AND from_unixtime(unix_timestamp(get_json_object(`data`, \'$.msgTime\')), \'yyyy-MM-dd HH:mm:ss\') >= \'2018-05-01 00:00:00\'\n" +
                "            AND from_unixtime(unix_timestamp(get_json_object(`data`, \'$.msgTime\')), \'yyyy-MM-dd HH:mm:ss\') <= \'2018-05-02 00:00:00\'), \n" +
                "charge_ele as\n" +
                "         (\n" +
                "             select\n" +
                "                 chargeInfo.vin,\n" +
                "                 chargeInfo.msgTime,\n" +
                "                 sum(chargeInfo.timeGap *chargeInfo.totalCurrent ) over (partition BY vin   ORDER BY msgTime) as chargeEle \n" +
                "             from (\n" +
                "                      SELECT tmp.vin,\n" +
                "                             tmp.msgTime,\n" +
                "                             CASE\n" +
                "                                 WHEN (tmp.msgTime - tmp.lastMsgTime) > 20 THEN 0\n" +
                "                                 ELSE (tmp.msgTime - tmp.lastMsgTime)/3600\n" +
                "                                 END AS timeGap,\n" +
                "                             tmp.totalCurrent\n" +
                "                      FROM\n" +
                "                          (SELECT vin,\n" +
                "                                  totalCurrent,\n" +
                "                                  msgTime,\n" +
                "                                  lag(msgTime, 1) over (partition BY vin\n" +
                "                                      ORDER BY msgTime ASC) AS lastMsgTime,\n" +
                "                                  chargeStatus\n" +
                "                           FROM charge_data) AS tmp\n" +
                "                      WHERE tmp.lastMsgTime IS NOT NULL\n" +
                "                        AND tmp.chargeStatus = \'1\') as chargeInfo \n" +
                "         )\n" +
                "select * from charge_ele";


        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql0);
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + " " + resultSet.getObject(2) + " " + resultSet.getObject(3));
        }
        resultSet.close();
        statement.close();
        connection.close();

    }

}
