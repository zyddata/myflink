package big13;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

//注意包

/**
 * 使用SQL查询数据
 */
public class TableSQLDemo {
	public static void main(String[] args) throws Exception {
		//得到默认执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> ds1 = env.readTextFile("file:///d:/mr/hello10.txt") ;

		//压扁并表一成对
		DataSet<Tuple2<String, Integer>> ds2 = ds1.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				for(String word : value.split(" ")){
					out.collect(new Tuple2<String,Integer>(word , 1) );
				}
			}
		}) ;
		//ds2.print();

		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		//注册表
		tEnv.registerDataSet("words" , ds2, "word , cnt");
		//通过sql查询注册的表
		Table t2 = tEnv.sqlQuery("select word , count(cnt) wcnt from words group by word order by wcnt desc") ;
		tEnv.toDataSet(t2 , Row.class).print(); ;
	}
}
