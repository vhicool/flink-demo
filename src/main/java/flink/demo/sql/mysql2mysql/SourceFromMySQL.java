package flink.demo.sql.mysql2mysql;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import flink.demo.sql.DataxTest;
import flink.demo.util.MySQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Desc: 自定义 source，从 mysql 中读取数据
 * Created by zhisheng on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/tags/Flink/
 */
public class SourceFromMySQL extends RichSourceFunction<DataxTest> {

    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://10.9.20.251:3306/datax_demo?useSSL=false&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
                "ztm2",
                "ztm2Test$!");
        String sql = "select * from t_test;";
        ps = this.connection.prepareStatement(sql);
    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * DataStream 调用一次 run() 方法用来获取数据
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<DataxTest> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            DataxTest student = new DataxTest(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("name1"));
            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {
    }
}
