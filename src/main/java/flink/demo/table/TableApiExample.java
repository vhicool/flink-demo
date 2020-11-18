package flink.demo.table;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * <p>Description: </p>
 *
 * @author lidawei
 * @date 2020/11/8
 * @since
 **/
public class TableApiExample {

	public static void main(String[] args) {
		//创建流运行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//设置并行度
		env.getConfig().setParallelism(4);

		//时间语义
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		//输入数据
		DataStreamSource<String> dataStreamSource =
				env.fromElements("Hello my name is li", "Hello my name is li", "name is li");

		StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
		//将流转换为表，并根据处理时间开窗
		Table table = streamTableEnvironment.fromDataStream(dataStreamSource, $("name"), $("time").proctime())
				.window(Tumble.over($("10").second()).on($("time")).as("tw"))
				.groupBy($("cnt"))
				.select($("name"), $("value").sum());


		streamTableEnvironment.createTemporaryView("text", dataStreamSource);
//		streamTableEnvironment.executeSql()
	}
}
