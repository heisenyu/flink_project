package com.dzj.utils.mysql;

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
                ")" + MysqlUtil.mysqlLookUpTableDDL("dzj","base_dic");
    }

    public static String getDoctorInfoLookUpDDL() {

        return "create table `doctor_info`(\n" +
                "`user_id` string,\n" +
                "`level` int,\n" +
                "primary key(`user_id`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("dzj","doctor_info");
    }

    public static String getVideoInfoLookUpDDL() {

        return "create table `video_info`(\n" +
                "`video_id` string,\n" +
                "`video_length` int,\n" +
                "primary key(`video_id`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("dzj","video_info");
    }


    public static String getCreditRuleConfig() {

        return "create table `credit_rule_config`(\n" +
                "`id` string,\n" +
                "`code` string,\n" +
                "`rule_code` string,\n" +
                "`doctor_level` int,\n" +
                "primary key(`id`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("tob","credit_rule_config");
    }

    //学分任务表
    public static String getCreditTask(){

        return "create table `credit_task`(\n" +
                "`id` string,\n" +
                "`code` string,\n" +
                "`rule_code` string,\n" +
                "`task_detail` string,\n" +
                "primary key(`id`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("tob","credit_task");
    }



    public static String mysqlLookUpTableDDL(String database,String tableName) {

        return "WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = '" + ComConfig.MYSQL_URL +database+ "',\n" +
                "'table-name' = '" + tableName + "',\n" +
                "'lookup.cache.max-rows' = '100',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +
                "'username' = '" + ComConfig.MYSQL_USER + "',\n" +
                "'password' = '" + ComConfig.MYSQL_PASSWORD + "',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
    }





}
