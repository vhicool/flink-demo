package flink.demo.word;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * <p>Description: 单词计数</p>
 *
 * @author lidawei
 * @date 2020/11/2
 * @since
 **/
public class WordCountMain {
	public static void main(String[] args) throws Exception {
		//创建流运行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//设置并行度
		env.getConfig().setParallelism(2);
		//检查点周期
		//env.enableCheckpointing(1000);
		//输入数据
		DataStreamSource<String> dataStreamSource = env.fromElements("Hello my name is li", "Hello my name is li", "name is li");
		//operator[1]
		dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				String[] splits = value.toLowerCase().split("\\W+");
				for (String split : splits) {
					if (split.length() > 0) {
						out.collect(new Tuple2<>(split, 1));
					}
				}
			}
		})
				//单独设置operator[1]的并行度
				.setParallelism(1)
				//根据key，将数据发送到不同subTasks
				.keyBy(0)
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
						return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
					}
				})
				.print();

		//Streaming 程序必须加这个才能启动程序，否则不会有结果
		env.execute("zhisheng —— flink.demo.word count streaming demo");
	}
}
