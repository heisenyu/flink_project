package com.dzj.utils.mysql;

/**
 * Author: zhangly
 * Date: 2023/4/24 17:58
 * FileName: MysqlBinlogWriter
 * Description: bin_log writter
 */
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlBinlogWriter extends RichSinkFunction<JSONObject> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinlogWriter.class);

    private Connection connection = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (connection == null) {
            Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
            connection = DriverManager.getConnection("jdbc:mysql://172.29.28.4:3306", "root", "123456");//获取连接
        }
//        ps = connection.prepareStatement("insert into ods_flink_cdc_test.ods_t_test values (?,?,?)");
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 获取binlog
        try {
            Integer id = (Integer) value.get("id");
            String username = (String) value.get("username");
            String password = (String) value.get("password");
            ps.setInt(1, id);
            ps.setString(2, username);
            ps.setString(3, password);
            ps.executeUpdate();
            LOGGER.info(ps.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
