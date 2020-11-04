package flink.demo.sql.mysql2mysql;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import flink.demo.sql.DataxTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Desc: sink 数据到 mysql
 * Created by yuanblog_tzs on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/tags/Flink/
 */
@Slf4j
public class SinkToMySQL extends RichSinkFunction<DataxTest> {
    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into t_test1(id, name, name1) values(?, ?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(DataxTest value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        //组装数据，执行插入操作
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setString(3, value.getName1());
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
            con = DriverManager.getConnection("jdbc:mysql://10.9.20.251:3306/datax_demo?useSSL=false&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
                    "ztm2", "ztm2Test$!");
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}
