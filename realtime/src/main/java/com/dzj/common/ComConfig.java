package com.dzj.common;

public class ComConfig {

    // Mysql 连接地址
    public static final String MYSQL_URL = "jdbc:mysql://172.29.28.186:3306/";

    //MYSQL 用户名
    public static final String MYSQL_USER = "root";

    //MYSQL 密码
    public static final String MYSQL_PASSWORD = "123456";


    //Redis 连接地址
    public static final String REDIS_HOST = "172.29.28.187";

    //Redis 端口
    public static final int REDIS_PORT = 6379;

    //Redis 密码
    public static final String REDIS_PASSWORD = "123456";


    //kafka连接参数
    public static final String KAFKA_BOOTSTRAP_SERVERS = "cdh001:9092,cdh002:9092,cdh003:9092";


}