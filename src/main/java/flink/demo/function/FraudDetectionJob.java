package flink.demo.function;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>Description: 信用卡欺诈demo，用于检测同一个账户id两个相邻的消费额，在1分钟之内小于1并且大于500的记录，进行告警</p>
 * <p>demo说明地址: https://ci.apache.org/projects/flink/flink-docs-master/try-flink/datastream_api.html</p>
 * @author lidawei
 * @date 2020/11/3
 * @since
 **/
public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Transaction> transactions = env
				.fromElements(new Transaction("1",1d),new Transaction("2",0.1d),new Transaction("2",1000d))
				.name("transactions");

		DataStream<FraudDetector.Alert> alerts = transactions
				.keyBy(Transaction::getAccountId)
				.process(new FraudDetector())
				.name("fraud-detector");
		alerts.addSink(new AlertSink())
				.name("send-alerts");

		env.execute("Fraud Detection");
	}

	static class Transaction{
		private String accountId;
		private double amount;

		public Transaction(String accountId, double amount) {
			this.accountId = accountId;
			this.amount = amount;
		}

		public String getAccountId() {
			return accountId;
		}

		public double getAmount() {
			return amount;
		}

		public void setAmount(double amount) {
			this.amount = amount;
		}
	}

}

