package flink.demo.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * <p>Description: </p>
 *
 * @author lidawei
 * @date 2020/10/27
 * @since
 **/
public class CsvMain {

	public static void main(String[] args) throws Exception {
		//第一步，创建上下文环境：
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);
		//读取 score.csv 并且作为 source 输入：
		String path = CsvMain.class.getClassLoader().getResource("score.csv").getPath();
		DataStreamSource<String> input = env.readTextFile(path);

		SingleOutputStreamOperator<PlayerData> topInput =
				input.map(new MapFunction<String, PlayerData>() {
					@Override
					public PlayerData map(String s) throws Exception {
						String[] split = s.split(",");
						return new PlayerData(String.valueOf(split[0]),
								String.valueOf(split[1]),
								String.valueOf(split[2]),
								Integer.valueOf(split[3]),
								Double.valueOf(split[4]),
								Double.valueOf(split[5]),
								Double.valueOf(split[6]),
								Double.valueOf(split[7]),
								Double.valueOf(split[8])
						);
					}
				});
		//第三步，将 source 数据注册成表：
		Table topScore = tableEnv.fromDataStream(topInput);
		tableEnv.registerTable("score", topScore);
		//第四步，核心处理逻辑 SQL 的编写：
		Table queryResult = tableEnv.sqlQuery("" +
				"select player, \n" +
				"count(season) as num \n" +
				"FROM score \n" +
				"GROUP BY player \n" +
				"ORDER BY num desc \n" +
				"LIMIT 3");
		//第五步，输出结果：
//		TableSink sink = new CsvTableSink("result.csv", ",");
//		String[] fieldNames = {"name", "num"};
//		TypeInformation[] fieldTypes = {Types.STRING, Types.INT};
//		tableEnv.registerTableSink("result", fieldNames, fieldTypes, sink);
//		queryResult.insertInto("result");
//		env.execute();
		//第五步，输出结果：
		DataStream<Result> result = tableEnv.toAppendStream(queryResult, Result.class);
		result.print();
	}

	public static class PlayerData {
		/**
		 * 赛季，球员，出场，首发，时间，助攻，抢断，盖帽，得分
		 */
		public String season;
		public String player;
		public String play_num;
		public Integer first_court;
		public Double time;
		public Double assists;
		public Double steals;
		public Double blocks;
		public Double scores;

		public PlayerData() {
			super();
		}

		public PlayerData(String season,
						  String player,
						  String play_num,
						  Integer first_court,
						  Double time,
						  Double assists,
						  Double steals,
						  Double blocks,
						  Double scores
		) {
			this.season = season;
			this.player = player;
			this.play_num = play_num;
			this.first_court = first_court;
			this.time = time;
			this.assists = assists;
			this.steals = steals;
			this.blocks = blocks;
			this.scores = scores;
		}
	}

	public static class Result {
		public String player;
		public Long num;

		public Result() {
			super();
		}

		public Result(String player, Long num) {
			this.player = player;
			this.num = num;
		}

		@Override
		public String toString() {
			return player + ":" + num;
		}
	}
}
