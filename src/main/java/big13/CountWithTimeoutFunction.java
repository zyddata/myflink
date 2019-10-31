package big13;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;

/**
 * @author zhangyide
 * @desc
 * @date 2019/5/2814:20
 */
public class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {

	/**
	 * 这个状态是通过过程函数来维护
	 */
	private ValueState<CountWithTimestamp> state;

	@Override
	public void open(Configuration parameters) throws Exception {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
		System.out.println("进入open方法");
	}

	@Override
	public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
		System.out.println("进入processElement方法");
		// 得到当前的计数
		CountWithTimestamp current = state.value();
		if (current == null) {
			current = new CountWithTimestamp();
			current.key = value.f0;
		}

		// 更新状态中的计数
		current.count++;

		// 设置状态中相关的时间戳
		current.lastModified = ctx.timestamp();

		// 状态回写
		state.update(current);

		// 从当前事件时间开始注册一个60s的定时器
		ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {

		// 得到设置这个定时器的键对应的状态
		CountWithTimestamp result = state.value();

		// check if this is an outdated timer or the latest timer
		if (timestamp == result.lastModified + 60000) {
			// emit the state on timeout
			out.collect(new Tuple2<String, Long>(result.key, result.count));
		}
	}
}