package flink.demo.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * <p>Description: 时间窗口</p>
 * <p>
 * allowedlateness 时间( P)=窗口的endtime+allowedlateness ，作为窗口被释放时间。globle window的默认allowedlateness 为Long.MAX_VALUE,其他窗口默认都是0，所以如果不配置allowedlateness 的话在水印触发了窗口计算后窗口被销毁
 * 假设窗口大小10s，水印允许延迟3s，allowedlateness 允许延迟6s
 * 窗口开始时间 0s
 * 窗口结束时间10s
 * 1.当水印时间13s时触发该窗口的计算，所以正常落在0-10s内的数据可以被窗口计算
 * 2.针对乱序情况，watermark = eventtime-3s，所以在watermark<10s前的乱序数据都可以落到窗口中计算
 * 比如数据eventTime依次为 1、4、8、2、12、6、13，此时2和6是乱序数据，但是都在13之前，所以1、4、5、2、6都可以落在0-10的窗口内在13到达时参与计算
 * 3.如果接入的数据的eventtime<P ，但watermark已经超过窗口的endtime时，会再次触发窗口的计算，每满足一条触发一次（前提是该数据属于这个窗口），如果eventtime超过了最大延迟时间P则丢弃，但是可以通过side output将丢弃的数据输出
 *
 * @author lidawei
 * @date 2020/11/5
 * @since
 **/
public class WordCountByWindowExample {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		//设置从此环境创建的所有流的时间特征为事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
		SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = dataStreamSource.flatMap(
				new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
						String[] splits = value.toLowerCase().split("\\W+");
						for (String split : splits) {
							if (split.length() > 0) {
								out.collect(new Tuple2<>(split, 1));
							}
						}
					}
				}
		);
		dataStream.keyBy(0)
				//5秒一个滚动窗口
				.timeWindow(Time.seconds(5))
				.allowedLateness(Time.seconds(2))
				//宽度为10秒，5秒滑动一次
//				.timeWindow(Time.seconds(10),Time.seconds(5))
				//5条记录后输出
//				.countWindow(5)
//				.countWindow(10,5)
				//容许2秒延迟
//				.allowedLateness(Time.seconds(2))
				.reduce(((value1, value2) -> new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1)))
				.print();
		env.execute("Word Count By Window");
	}
}
