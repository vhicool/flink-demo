package flink.demo.sql;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * <p>Description: 从mysql到kafka全量同步，可以在DDL中设置事件时间，和Window
 * @author lidawei
 * @date 2020/10/30
 * @since
 **/
public class Jdbc2KafkaWithTimeWindowExample {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		blinkStreamEnv.setParallelism(3);
		blinkStreamEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv);

		String ddlSource = "CREATE TABLE user_behavior (\n" +
				"    user_id BIGINT,\n" +
				"    item_id BIGINT,\n" +
				"    category_id BIGINT\n" +
				"    ts TIMESTAMP(3)\n" +
				") WITH (\n" +
				"    'connector.type' = 'kafka',\n" +
				"    'connector.version' = '0.11',\n" +
				"    'connector.topic' = 'test_dev_kafka_flink',\n" +
				"    'connector.startup-mode' = 'latest-offset',\n" +
				"    'connector.properties.zookeeper.connect' = '10.9.20.100:2181',\n" +
				"    'connector.properties.bootstrap.servers' = '10.9.20.38:9092',\n" +
				"    'format.type' = 'json'\n" +
				")";

		String ddlSink = "CREATE TABLE user_behavior_sink (\n" +
				"    user_id BIGINT,\n" +
				"    times INT,\n" +
				"    ts TIMESTAMP(3),\n" +
				// 定义事件时间
				"    rt Ad TO_TIMESTAMP(FROM UNIXTIME(ts)),\n" +//如果ts不是TIMESTAMP(3)，需要转化为TIMESTAMP(3)
				"	 watermark for rt as rt - interval '1' second \n" +// watermark延迟1秒
//				定义处理时间
//				"	 pt AD PROCTIME()\n"+
				") WITH (\n" +
				"    'connector.type' = 'jdbc',\n" +
				"    'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
				"    'connector.url' = 'jdbc:mysql://10.9.20.251:3306/zdtp-test',\n" +
				"    'connector.table' = 'user', \n" +
				"    'connector.username' = 'ztm2', \n" +
				"    'connector.password' = 'ztm2Test$!',\n" +
				"    'connector.write.flush.max-rows' = '1' \n" +
				")";

		String kafka2jdbc = "insert into user_behavior_sink (select user_id,count(user_id) OVER(" +
				"PARTITION BY user_id " +
				"ORDER BY ts "+
				"ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"+
				") from user_behavior group by user_id)";
		blinkStreamTableEnv.executeSql(ddlSource);
		blinkStreamTableEnv.executeSql(ddlSink);
		blinkStreamTableEnv.executeSql(kafka2jdbc).print();

//		blinkStreamTableEnv.execute("Blink Stream SQL demo PG");
	}
}
