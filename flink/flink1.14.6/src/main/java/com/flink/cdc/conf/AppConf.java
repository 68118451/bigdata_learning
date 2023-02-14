package com.flink.cdc.conf;


import com.flink.conf.CommonConf;

/**
 * description: Appconf <br>
 * date: 2022-12-05 16:08 <br>
 * author: YQ <br>
 * version: 1.0 <br>
 */
public class AppConf extends CommonConf {

    public AppConf() {
        load();
    }

    private static  final String AZKABAN_MYSQL_JDBC_URL="azkaban.mysql.jdbc.url";
    private static  final String AZKABAN_MYSQL_USER_NAME="azkaban.mysql.jdbc.username";
    private static  final String AZKABAN_MYSQL_PASSWORD="azkaban.mysql.jdbc.password";
    private static  final String MAYBE_MONGO_HOST="maybe.mongo.host";
    private static  final String PEANUT_MONGO_HOST="peanut.mongo.host";
    private static  final String KUDU_MASTER="kudu.master";
    private static  final String MONGO_CDC_USER="mongo.cdc.user";
    private static  final String MONGO_CDC_PASSWORD="mongo.cdc.password";
    private static  final String PEANUT_MYSQL_JDBC_URL="peanut.mysql.jdbc.url";



    public String getAzkabanMysqlJdbcUrl() {
        return param().get(AZKABAN_MYSQL_JDBC_URL);
    }

    public String getAzkabanMysqlUserName() {
        return param().get(AZKABAN_MYSQL_USER_NAME);
    }

    public String getAzkabanMysqlPassword() {
        return param().get(AZKABAN_MYSQL_PASSWORD);
    }

    public  String getKuduMaster() {
      return param().get(KUDU_MASTER);
    }

    public  String getMaybeMongoHost() {
        return param().get(MAYBE_MONGO_HOST);
    }

    public  String getPeanutMongoHost() {
        return param().get(PEANUT_MONGO_HOST);
    }


    public  String getMongoCdcUser() {
        return param().get(MONGO_CDC_USER);
    }

    public  String getMongoCdcPassword() {
        return param().get(MONGO_CDC_PASSWORD);
    }

    public  String getPeanutMysqlJdbcUrl() {
        return param().get(PEANUT_MYSQL_JDBC_URL);
    }

}
