package com.dzj.common;

public class ComConfig {

    // Mysql 连接地址
    public static final String MYSQL_URL = "jdbc:mysql://172.29.28.186:3306/";

    //MYSQL 用户名
    public static final String MYSQL_USER = "root";

    //MYSQL 密码
    public static final String MYSQL_PASSWORD = "123456";


    //kafka连接参数
    public static final String KAFKA_BOOTSTRAP_SERVERS = "cdh001:9092,cdh002:9092,cdh003:9092";
    ;

    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL211126_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/gmall_211126";


}