package flink.demo.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * <p>Description: 从mysql到kafka全量同步</p>
 *
 * @author lidawei
 * @date 2020/10/30
 * @since
 **/
public class Jdbc2KafkaExample {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		blinkStreamEnv.setParallelism(1);
		StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv);

		String ddlSource = "CREATE TABLE user_behavior (\n" +
				"    user_id BIGINT,\n" +
				"    item_id BIGINT,\n" +
				"    category_id BIGINT\n" +
//				"    behavior STRING,\n" +
//				"    ts TIMESTAMP(3)\n" +
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
				"    item_id BIGINT,\n" +
				"    category_id BIGINT\n" +
				") WITH (\n" +
				"    'connector.type' = 'jdbc',\n" +
				"    'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
				"    'connector.url' = 'jdbc:mysql://10.9.20.251:3306/zdtp-test',\n" +
				"    'connector.table' = 'user', \n" +
				"    'connector.username' = 'ztm2', \n" +
				"    'connector.password' = 'ztm2Test$!',\n" +
				"    'connector.write.flush.max-rows' = '1' \n" +
				")";

//		String kafka2jdbc = "insert into user_behavior_sink select user_id,item_id,category_id from user_behavior";
		String jdbc2kafka = "insert into user_behavior select user_id,item_id,category_id from user_behavior_sink";


		blinkStreamTableEnv.executeSql(ddlSource);
		blinkStreamTableEnv.executeSql(ddlSink);
		blinkStreamTableEnv.executeSql(jdbc2kafka);

//		blinkStreamTableEnv.execute("Blink Stream SQL demo PG");
	}
}
