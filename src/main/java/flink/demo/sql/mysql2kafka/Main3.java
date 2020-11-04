package flink.demo.sql.mysql2kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import flink.demo.sql.mysql2mysql.SourceFromMySQL;

/**
 * Desc: 自定义 source，从 mysql 中读取数据
 * Created by zhisheng on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/tags/Flink/
 */
public class Main3 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMySQL())
                .addSink(new FlinkKafkaProducer011(
                "10.9.45.97:9092,10.9.15.38:9092,10.9.36.49:9092,10.9.36.50:9092",
                "test_kafka_flink",
                new StudentSchema()
        )).name("flink-stream-mysql-kafka").setParallelism(1);

        env.execute("flink stream mysql -> kafka");
    }
}
