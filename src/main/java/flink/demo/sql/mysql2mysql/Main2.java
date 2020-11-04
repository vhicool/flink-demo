package flink.demo.sql.mysql2mysql;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import flink.demo.sql.DataxTest;

/**
 * Desc: 自定义 source，从 mysql 中读取数据
 * Created by zhisheng on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/tags/Flink/
 */
public class Main2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<DataxTest> dataStreamSink = env.addSource(new SourceFromMySQL());

        dataStreamSink.addSink(new SinkToMySQL()); //数据 sink 到 mysql

        env.execute("Flink mysql data sourc -> mysql data sink");
    }
}
