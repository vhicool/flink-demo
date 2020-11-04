package flink.demo.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * <p>Description: </p>
 *
 * @author lidawei
 * @date 2020/10/30
 * @since
 **/
public class Jdbc2EsExample {
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		blinkStreamEnv.setParallelism(1);
		StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv);

		String ddlSource = "CREATE TABLE user_behavior (\n" +
				"    user_id BIGINT,\n" +
				"    item_id BIGINT,\n" +
				"    category_id BIGINT\n" +
				") WITH (\n" +
				"    'connector.type' = 'jdbc',\n" +
				"    'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
				"    'connector.url' = 'jdbc:mysql://localhost:3306/zdtp_test',\n" +
				"    'connector.table' = 'user', \n" +
				"    'connector.username' = 'root', \n" +
				"    'connector.password' = '123456',\n" +
				"    'connector.write.flush.max-rows' = '1' \n" +
				")";
		String ddlSink = "CREATE TABLE user_behavior_es (\n" +
				"    user_id BIGINT,\n" +
				"    item_id BIGINT\n" +
				") WITH (\n" +
				"    'connector.type' = 'elasticsearch',\n" +
				"    'connector.version' = '6',\n" +
				"    'connector.hosts' = 'http://localhost:9200',\n" +
				"    'connector.index' = 'user_behavior_es',\n" +
				"    'connector.document-type' = 'user_behavior_es',\n" +
				"    'format.type' = 'json',\n" +
				"    'update-mode' = 'append',\n" +
				"    'connector.bulk-flush.max-actions' = '10'\n" +
				")";

		String sql = "insert into user_behavior_es select user_id, item_id from user_behavior";

		blinkStreamTableEnv.executeSql(ddlSource);
		blinkStreamTableEnv.executeSql(ddlSink);
		blinkStreamTableEnv.executeSql(sql);

//		blinkStreamTableEnv.execute("Blink Stream SQL Job2 —— read data from kafka，sink to es");
	}
}
