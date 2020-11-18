package flink.demo.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * <p>Description: 从mysql到kafka全量同步</p>
 *
 * @author lidawei
 * @date 2020/10/30
 * @since
 **/
public class Jdbc2PrintExample {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		blinkStreamEnv.setParallelism(3);
		StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv);

		String user_print = "CREATE TABLE user_print (\n" +
				"    user_id BIGINT,\n" +
				"    item_id BIGINT,\n" +
				"    category_id BIGINT,\n" +
				"    id_card VARCHAR,\n" +
				"    password VARCHAR\n" +
				") WITH (\n" +
				"    'connector' = 'print'\n" +
				")";

		String user_print2 = "CREATE TABLE user_print2 (\n" +
				"    user_id BIGINT,\n" +
				"    cnt BIGINT\n" +
				") WITH (\n" +
				"    'connector' = 'print'\n" +
				")";


		String user_print3 = "CREATE TABLE user_print3 (\n" +
				"    user_id BIGINT,\n" +
				"    item_id BIGINT,\n" +
				"    category_id BIGINT\n" +
				") WITH (\n" +
				"    'connector' = 'print'\n" +
				")";
		String ddlSink = "CREATE TABLE user_table (\n" +
				"    user_id BIGINT,\n" +
				"    item_id BIGINT,\n" +
				"    category_id BIGINT,\n" +
				"    id_card VARCHAR,\n" +
				"    password VARCHAR\n" +
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
//		String jdbc2Print = "insert into user_print select user_id,item_id,category_id from user_table";
//		String jdbc2Print2 = "insert into user_print2 select user_id,count(user_id) from user_table group by user_id";
		String jdbc2Print3 = "insert into user_print  select user_id,item_id,category_id ,SUBSTRING(id_card,-6),MD5(password || '-secret')from user_table";
		blinkStreamTableEnv.executeSql(user_print);
//		blinkStreamTableEnv.executeSql(user_print);
//		blinkStreamTableEnv.executeSql(user_print);
//		blinkStreamTableEnv.executeSql(user_print2);
//		blinkStreamTableEnv.executeSql(user_print3);

		blinkStreamTableEnv.executeSql(ddlSink);
		blinkStreamTableEnv.executeSql(jdbc2Print3);

	}
}
