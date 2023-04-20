package com.dzj.app.base;

import com.dzj.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class demo {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(MysqlUtil.getDoctorInfoLookUpDDL());

        System.out.println(MysqlUtil.getDoctorInfoLookUpDDL());

        tableEnv.executeSql("SHOW CATALOGS").print();

//        tableEnv.sqlQuery("select * from doctor_info").execute().print();
    }


}
