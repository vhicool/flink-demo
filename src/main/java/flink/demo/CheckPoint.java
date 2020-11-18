package flink.demo;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * <p>Description: 状态编程</p>
 *
 * @author lidawei
 * @date 2020/11/5
 * @since
 **/
public class CheckPoint {
	public static void main(String[] args) throws IOException {
		//创建流运行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//检查点间隔毫秒数
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		//checkpoint超时时间
		env.getCheckpointConfig().setCheckpointTimeout(100000);
		//最大checkpoint并发数
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
		//checkpoint外部持久化
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.setStateBackend(new RocksDBStateBackend("/data/flink/rocksdb"));
		//重启策略
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
	}
}
