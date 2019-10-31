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
public class MaxTempBatchJava {
	public static void main(String[] args) throws Exception {
		//得到默认执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment() ;

		//读取文件
		DataSet<String> ds1 = env.readTextFile("file:///d:/mr/temp3.dat") ;

		//(year , temp)
		DataSet<Tuple2<Integer, Integer>> ds2 = ds1.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
			public Tuple2<Integer, Integer> map(String value) throws Exception {
				String[] arr = value.split(" ") ;
				int year = Integer.parseInt(arr[0]) ;
				int temp = Integer.parseInt(arr[1]) ;
				return new Tuple2<Integer,Integer>(year , temp) ;
			}
		}) ;

		UnsortedGrouping<Tuple2<Integer, Integer>> ds3=  ds2.groupBy("f0") ;
		ds3.max(1).print(); ;

	}
}
