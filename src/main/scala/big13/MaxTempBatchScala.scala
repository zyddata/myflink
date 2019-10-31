package big13

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * 使用flink实现最高气温的batch计算
  */
object MaxTempBatchScala {
    def main(args: Array[String]): Unit = {
        //显式导入隐式转换
        import org.apache.flink.streaming.api.scala._

        val benv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        //读取文件
        val ds1: DataSet[String] = benv.readTextFile("file:///d:/mr/temp3.dat")

        //
        val ds2 = ds1.map(line => {
            val arr = line.split(" ")
            (arr(0).toInt , arr(1).toInt)
        }) ;
        //分组
        val ds3 = ds2.groupBy("_1")

        val ds4 = ds3.max("_2")

        ds4.sortPartition("_1", org.apache.flink.api.common.operators.Order.ASCENDING).print()
    }
}
