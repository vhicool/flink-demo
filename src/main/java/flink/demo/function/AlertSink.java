package flink.demo.function;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * <p>Description: </p>
 *
 * @author lidawei
 * @date 2020/11/3
 * @since
 **/
public class AlertSink implements SinkFunction<FraudDetector.Alert> {
	@Override
	public void invoke(FraudDetector.Alert value, Context context){
		System.out.println(value);
	}
}
