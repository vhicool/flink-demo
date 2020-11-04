package flink.demo.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * <p>Description: </p>
 *
 * @author lidawei
 * @date 2020/11/3
 * @since
 **/
public class FraudDetector extends KeyedProcessFunction<String, FraudDetectionJob.Transaction, FraudDetector.Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	/**
	 * 每个key有不同的ValueState
	 */
	private transient ValueState<Boolean> flagState;
	private transient ValueState<Long> timerState;

	@Override
	public void open(Configuration parameters) {
		//标记值
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);
		//当前时间
		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
				"timer-state",
				Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}
	@Override
	public void processElement(
			FraudDetectionJob.Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		// Get the current state for the current key
		Boolean lastTransactionWasSmall = flagState.value();

		// Check if the flag is set
		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				//Output an alert downstream
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());

				collector.collect(alert);
			}
			// Clean up our state
			cleanUp(context);
		}

		if (transaction.getAmount() < SMALL_AMOUNT) {
			// set the flag to true
			flagState.update(true);

			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerProcessingTimeTimer(timer);

			timerState.update(timer);
		}

	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
		// remove flag after 1 minute
		//定时器触发事件
		timerState.clear();
		flagState.clear();
	}

	/**
	 * 清除时间触发器
	 */
	private void cleanUp(Context ctx) throws Exception {
		// delete timer
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		// clean up all state
		timerState.clear();
		flagState.clear();
	}

	class Alert {
		private String id;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		@Override
		public String toString() {
			return "Alert{" +
					"id='" + id + '\'' +
					'}';
		}
	}
}

