package com.dzj.utils;

import com.dzj.common.ComConfig;

public class MysqlUtil {

    public static String getDoctorInfoLookUpDDL() {

        return "create table `doctor_info`(\n" +
                "`user_id` string,\n" +
                "`level` int,\n" +
                "primary key(`user_id`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("dzj", "doctor_info");
    }

    public static String getVideoInfoLookUpDDL() {

        return "create table `video_info`(\n" +
                "`video_id` string,\n" +
                "`video_length` int,\n" +
                "primary key(`video_id`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("dzj", "video_info");
    }

    public static String getCreditTaskLookUpDDL() {

        return "create table `credit_task`(\n" +
                "`code` string,\n" +
                "`rule_code` string,\n" +
                "`task_detail` string\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("tob", "credit_task");
    }


    public static String getCreditRuleConfigLookUpDDL() {

        return "create table `credit_rule_config`(\n" +
                "`rule_code` string,\n" +
                "`doctor_level` int\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("tob", "credit_rule_config");
    }

    public static String mysqlLookUpTableDDL(String dataBase, String tableName) {

        return "WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = '" + ComConfig.MYSQL_URL + dataBase + "',\n" +
                "'table-name' = '" + tableName + "',\n" +
                "'lookup.cache.max-rows' = '10',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +
                "'username' = '" + ComConfig.MYSQL_USER + "',\n" +
                "'password' = '" + ComConfig.MYSQL_PASSWORD + "',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
    }
}
