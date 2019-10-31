package big13;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 
 */
public class WordCountStreamJava {
	public static void main(String[] args) throws Exception {
		//创建流执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//socket文本流
		DataStream<String> ds1 =  env.socketTextStream("192.168.72.101" , 8888 , "\n") ;

		//对行进行压扁
		DataStream<String> ds2 = ds1.flatMap(new FlatMapFunction<String, String>() {
			public void flatMap(String value, Collector<String> out) throws Exception {
				for(String word : value.split(" ")){
					out.collect(word);
				}
			}
		}) ;

		//标一成对
		DataStream<Tuple2<String, String>> ds3 = ds2.map(
				new MapFunction<String, Tuple2<String, String>>() {
					public Tuple2<String, String> map(String word) throws Exception {
						return new Tuple2<String, String>(word, "1");
					}
				});

		//按照word分组
		DataStream<Tuple2<String, Long>> ds4 = ds3.keyBy(0).process(
				new CountWithTimeoutFunction());


/*		AllWindowedStream<WordWithCount, TimeWindow> ds5 = ds4.timeWindowAll(Time.seconds(5)) ;

		SingleOutputStreamOperator<Map<String, String>> ds6 = ds5.aggregate(new MyAggregationFunction());*/

//		SingleOutputStreamOperator<WordWithCount> ds6 = ds5.reduce(new ReduceFunction<WordWithCount>() {
//			public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
//				return new WordWithCount(value1.word , value1.count + value2.count);
//			}
//		}) ;

		ds4.print().setParallelism(1) ;

		env.execute("Socket Window WordCount");
	}

	public static class MyAggregationFunction implements AggregateFunction<WordWithCount,Map<String,String>, Map<String, String>>{


		@Override
		public Map<String, String> createAccumulator() {
			return new HashMap<>();
		}

		@Override
		public Map<String, String> add(WordWithCount wordWithCount, Map<String, String> map) {
			String word = wordWithCount.word;
			if (!map.keySet().contains(word)) {
				map.put(word, String.valueOf(1));
			} else {
				String s = map.get(word);
				int count = Integer.parseInt(s) + 1;
				map.put(word, String.valueOf(count));
			}
			return map;
		}

		@Override
		public Map<String, String> getResult(Map<String, String> map) {
			return map;
		}

		@Override
		public Map<String, String> merge(Map<String, String> map, Map<String, String> acc1) {
			Set<String> stringSet = map.keySet();
			for(String keyMap:stringSet){
				if (acc1.keySet().contains(keyMap)){
					int finalCount = Integer.parseInt(map.get(keyMap)) + Integer.parseInt(acc1.get(keyMap));
					map.put(keyMap,String.valueOf(finalCount));
				}
			}
			return map;
		}
	}
	/**
	 * 定义javabean
	 */
	public static class WordWithCount {
		public String word;
		public long count;
		public WordWithCount() {
		}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		public String toString() {
			return word + " : " + count;
		}
	}
}
