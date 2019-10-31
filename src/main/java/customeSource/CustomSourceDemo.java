package customeSource;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhangyide
 * @desc
 * @date 2019/4/1120:21
 */
public class CustomSourceDemo {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Long> text = env.addSource(new myCustomeSource());
		SingleOutputStreamOperator<Long> num = text.map(new MapFunction<Long, Long>() {

			@Override
			public Long map(Long value) throws Exception {
				System.out.println("收到的数据是：" + value);
				if(value==1000L){
					new myCustomeSource().cancel();
				}

				return value;
			}
		});

		SingleOutputStreamOperator<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
		sum.print().setParallelism(1);

		env.execute("自定义source");
	}
}
