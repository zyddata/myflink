package big13;

import akka.dispatch.Foreach;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
//注意包
import org.apache.flink.table.api.GroupedTable;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import javax.xml.bind.SchemaOutputResolver;
import java.util.Map;

/**
 *
 */
public class TableAPIDemo {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<String> data = env.readTextFile("D:\\user\\hive\\warehouse\\dm_dmp.db\\dm_keyword_19_csv\\dm_keyword_19");

		data.filter(new FilterFunction<String>() {
			public boolean filter(String value) {
				return value.startsWith("http://");
			}
		});

		JobExecutionResult res = env.execute("====");
	}
}
