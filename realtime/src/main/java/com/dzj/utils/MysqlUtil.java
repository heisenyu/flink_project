package com.dzj.utils;

import com.dzj.common.ComConfig;

public class MysqlUtil {
    public static String getBaseDicLookUpDDL() {

        return "create table `base_dic`(\n" +
                "`dic_code` string,\n" +
                "`dic_name` string,\n" +
                "`parent_code` string,\n" +
                "`create_time` timestamp,\n" +
                "`operate_time` timestamp,\n" +
                "primary key(`dic_code`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String getDoctorInfoLookUpDDL() {

        return "create table `doctor_info`(\n" +
                "`user_id` string,\n" +
                "`level` int,\n" +
                "primary key(`user_id`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("doctor_info");
    }

    public static String getVideoInfoLookUpDDL() {

        return "create table `video_info`(\n" +
                "`video_id` string,\n" +
                "`video_length` int,\n" +
                "primary key(`video_id`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("video_info");
    }

    public static String mysqlLookUpTableDDL(String tableName) {

        return "WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = '" + ComConfig.MYSQL_URL + "',\n" +
                "'table-name' = '" + tableName + "',\n" +
                "'lookup.cache.max-rows' = '10',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +
                "'username' = '" + ComConfig.MYSQL_USER + "',\n" +
                "'password' = '" + ComConfig.MYSQL_PASSWORD + "',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
    }
}
