package big13;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * flink实现batch处理，java实现word count
 */
public class WordCountBatchJava {
	public static void main(String[] args) throws Exception {
		//得到默认执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment() ;

		//读取文件
		DataSet<String> ds1 = env.readTextFile("file:///d:/hello10.txt") ;

		//压扁
		DataSet<String> ds2 = ds1.flatMap(new FlatMapFunction<String, String>() {
			public void flatMap(String value, Collector<String> out) throws Exception {
				for(String str : value.split(" ")){
					out.collect(str);
				}
			}
		}) ;

		//标一成对
		DataSet<Tuple2<String, Long>> ds3 = ds2.map(new MapFunction<String, Tuple2<String,Long>>() {
			public Tuple2<String, Long> map(String value) throws Exception {
				return new Tuple2<String, Long>(value , 1L) ;
			}
		}) ;

		//分组
		UnsortedGrouping<Tuple2<String, Long>> ds4 = ds3.groupBy("f0") ;

		//统计单词的个数
		ds4.sum(1).print();
	}
}
