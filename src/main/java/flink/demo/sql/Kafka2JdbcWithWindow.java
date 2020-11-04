package flink.demo.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.apache.flink.table.api.Expressions.*;

/**
 * <p>Description: https://ci.apache.org/projects/flink/flink-docs-master/try-flink/table_api.html</p>
 *
 * @author lidawei
 * @date 2020/11/3
 * @since
 **/
public class Kafka2JdbcWithWindow {
	public static void main(String[] args) {
		StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		blinkStreamEnv.setParallelism(1);
		StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv);

		blinkStreamTableEnv.executeSql("CREATE TABLE transactions (\n" +
				"    account_id  BIGINT,\n" +
				"    amount      BIGINT,\n" +
				"    transaction_time TIMESTAMP(3),\n" +
				"    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
				") WITH (\n" +
				"    'connector' = 'kafka',\n" +
				"    'topic'     = 'transactions',\n" +
				"    'properties.bootstrap.servers' = 'kafka:9092',\n" +
				"    'format'    = 'csv'\n" +
				")");

		blinkStreamTableEnv.executeSql("CREATE TABLE spend_report (\n" +
				"    account_id BIGINT,\n" +
				"    log_ts     TIMESTAMP(3),\n" +
				"    amount     BIGINT\n," +
				"    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
				") WITH (\n" +
				"    'connector'  = 'jdbc',\n" +
				"    'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
				"    'table-name' = 'spend_report',\n" +
				"    'driver'     = 'com.mysql.jdbc.Driver',\n" +
				"    'username'   = 'sql-demo',\n" +
				"    'password'   = 'demo-sql'\n" +
				")");

		Table transactions = blinkStreamTableEnv.from("transactions");
		report(transactions).executeInsert("spend_report");
	}


	public static Table report(Table transactions) {
		return transactions.select(
				$("account_id"),
				$("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
				$("amount"))
				.groupBy($("account_id"), $("log_ts"))
				.select(
						$("account_id"),
						$("log_ts"),
						$("amount").sum().as("amount"));
	}

	/**
	 * 自定义函数
	 * @param transactions
	 * @return
	 */
	public static Table report2(Table transactions) {
		return transactions.select(
				$("account_id"),
				call(MyFloor.class, $("transaction_time")).as("log_ts"),
				$("amount"))
				.groupBy($("account_id"), $("log_ts"))
				.select(
						$("account_id"),
						$("log_ts"),
						$("amount").sum().as("amount"));
	}


	/**
	 * 基于时间窗口输出
	 * @param transactions
	 * @return
	 */
	public static Table report3(Table transactions) {
		return transactions
				.window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("log_ts"))
				.groupBy($("account_id"), $("log_ts"))
				.select(
						$("account_id"),
						$("log_ts").start().as("log_ts"),
						$("amount").sum().as("amount"));
	}

	class MyFloor extends ScalarFunction {

		public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(
				@DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {

			return timestamp.truncatedTo(ChronoUnit.HOURS);
		}
	}
}
